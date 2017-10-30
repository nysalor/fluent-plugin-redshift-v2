class Fluent::Plugin::RedshiftOutputV2 < Fluent::BufferedOutput
  Fluent::Plugin.register_output('redshift_v2', self)

  attr_reader :last_sql, :last_gz_path

  config_param :record_log_tag, :string, default: 'log'

  # s3
  config_param :aws_key_id, :string, secret: true, default: nil,
               desc: "AWS access key id to access s3 bucket."
  config_param :aws_sec_key, :string, secret: true, default: nil,
               desc: "AWS secret key id to access s3 bucket."
  config_param :aws_iam_role, :string, secret: true, default: nil,
               desc: "AWS IAM Role to access s3 bucket."
  config_param :s3_region, :string,
               desc: 'AWS region name.'
  config_param :s3_bucket, :string,
               desc: 'bucket name. S3 bucket must be same as the region of your Redshift cluster.'
  config_param :s3_endpoint, :string, default: nil,
               desc: "S3 endpoint."
  config_param :path, :string, default: "",
               desc: "S3 path to input."
  config_param :timestamp_key_format, :string, default: 'year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M',
               desc: 'The format of the object keys. It can include date-format directives.'
  config_param :utc, :bool, default: false
  config_param :s3_server_side_encryption, :string, default: nil,
               desc: "S3 Server-Side Encryption (Only aes256 is supported)."

  # redshift
  config_param :redshift_host, :string,
               desc: "The end point(or hostname) of your Amazon Redshift cluster."
  config_param :redshift_port, :integer, default: 5439,
               desc: "Port number."
  config_param :redshift_dbname, :string,
               desc: "Database name."
  config_param :redshift_user, :string,
               desc: "User name."
  config_param :redshift_password, :string, secret: true,
               desc: "Password for the user name."
  config_param :redshift_tablename, :string,
               desc: "Table name to store data."
  config_param :redshift_schemaname, :string, default: nil,
               desc: 'Schema name to store data. By default, this option is not set and find table without schema as your own search_path.'
  config_param :redshift_copy_base_options, :string , default: "ESCAPE FILLRECORD ACCEPTANYDATE TRUNCATECOLUMNS"
  config_param :redshift_copy_options, :string , default: nil
  config_param :redshift_connect_timeout, :integer, default: 10,
               desc: "Maximum time to wait for connection to succeed."
  config_param :redshift_copy_columns, :string, default: nil,
               desc: 'Columns for copying. Value needs to be comma-separated like id,name,age'

  # file format
  config_param :file_type, :string, default: nil,
               desc: "File format of the source data. csv, tsv, msgpack or json are available."
  config_param :delimiter, :string, default: nil,
               desc: 'Delimiter of the source data. This option will be ignored if file_type is specified. '

  # for debug
  config_param :log_suffix, :string, default: ''

  def initialize
    super

    require 'aws-sdk'
    require 'zlib'
    require 'time'
    require 'tempfile'
    require 'pg'
    require 'json'
    require 'csv'
  end

  def configure(conf)
    super
    if !check_credentials
      fail ConfigError, "aws_key_id and aws_sec_key is required. or, use aws_iam_role instead."
    end
    @path = "#{@path}/" unless @path.end_with?('/') # append last slash
    @path = @path[1..-1] if @path.start_with?('/')  # remove head slash
    @utc = true if conf['utc']
    @db_conf = {
      host: @redshift_host,
      port: @redshift_port,
      dbname: @redshift_dbname,
      user: @redshift_user,
      password: @redshift_password,
      connect_timeout: @redshift_connect_timeout,
      hostaddr: IPSocket.getaddress(@redshift_host)
    }
    @delimiter = determine_delimiter(@file_type) if @delimiter.nil? or @delimiter.empty?
    $log.debug format_log("redshift file_type:#{@file_type} delimiter:'#{@delimiter}'")
    @table_name_with_schema = [@redshift_schemaname, @redshift_tablename].compact.join('.')
    @redshift_copy_columns = if !@redshift_copy_columns.to_s.empty?
                               @redshift_copy_columns.split(/[,\s]+/)
                             else
                               nil
                             end
    @copy_sql_template = build_redshift_copy_sql_template
    @s3_server_side_encryption = @s3_server_side_encryption.to_sym if @s3_server_side_encryption
  end

  def start
    super

    options = {}
    if @aws_key_id && @aws_sec_key
      options = {
        access_key_id: @aws_key_id,
        secret_access_key: @aws_sec_key,
        force_path_style: true,
        region: @s3_region
      }
    end
    options[:endpoint] = @s3_endpoint if @s3_endpoint
    @s3_client = Aws::S3::Client.new(options)
    @redshift_connection = RedshiftConnection.new(@db_conf)
  end

  def shutdown
  end

  def format(_tag, _time, record)
    if json?
      record.to_msgpack
    elsif msgpack?
      { @record_log_tag => record }.to_msgpack
    else
      "#{record[@record_log_tag]}\n"
    end
  end

  def write(chunk)
    insert_logs chunk
  end

  def insert_logs(chunk)
    $log.debug format_log("start creating gz.")
    exec_copy s3_uri(create_gz_file(chunk))
  end

  def create_gz_file(chunk)
    tmp = Tempfile.new("s3-")
    tmp =
      if json? || msgpack?
        create_gz_file_from_structured_data(tmp, chunk)
      else
        create_gz_file_from_flat_data(tmp, chunk)
      end

    if tmp
      key = next_gz_path
      @s3_client.put_object({
          acl: :bucket_owner_full_control,
          server_side_encryption: @s3_server_side_encryption,
          bucket: @s3_bucket,
          body: tmp,
          key: key
        })

      tmp.close!
      @last_gz_path = key
    else
      $log.debug format_log("received no valid data. ")
      return false
    end
  end

  def next_gz_path
    timestamp_key = (@utc) ? Time.now.utc.strftime(@timestamp_key_format) : Time.now.strftime(@timestamp_key_format)
    i = 0
    path = ''
    loop do
      path = "#{@path}#{timestamp_key}_#{'%02d' % i}.gz"
      begin
        @s3_client.head_object(key: path, bucket: @s3_bucket)
        i += 1
      rescue Aws::S3::Errors::NotFound
        break
      end
    end
    path
  end

  def exec_copy(s3_uri)
    $log.debug format_log("start copying. s3_uri=#{s3_uri}")
    begin
      @redshift_connection.exec copy_sql(s3_uri)
      $log.info format_log("completed copying to redshift. s3_uri=#{s3_uri}")
      true
    rescue RedshiftError => e
      if e.to_s =~ IGNORE_REDSHIFT_ERROR_REGEXP
        $log.error format_log("failed to copy data into redshift due to load error. s3_uri=#{s3_uri}"), error:e.to_s
        return false
      end
      raise e
    end
  end

  def s3_uri(path)
    "s3://#{@s3_bucket}/#{path}"
  end

  def copy_sql(s3_uri)
    @last_sql = @copy_sql_template % s3_uri
  end

  def format_log(message)
    if @log_suffix && !@log_suffix.empty?
      "#{message} #{@log_suffix}"
    else
      message
    end
  end

  private

  def check_credentials
    if @aws_key_id && @aws_sec_key
      true
    elsif @aws_iam_role
      true
    else
      false
    end
  end

  def determine_delimiter(file_type)
    case file_type
    when 'json', 'msgpack', 'tsv'
      "\t"
    when "csv"
      ','
    else
      raise Fluent::ConfigError, "Invalid file_type:#{file_type}."
    end
  end

  def build_redshift_copy_sql_template
    copy_columns = if @redshift_copy_columns
                     "(#{@redshift_copy_columns.join(",")})"
                   else
                     ''
                   end
    credentials = if @aws_key_id && @aws_sec_key
                    "CREDENTIALS 'aws_access_key_id=#{@aws_key_id};aws_secret_access_key=#{@aws_sec_key}'"
                  else
                    "CREDENTIALS 'aws_iam_role=#{@aws_iam_role}'"
                  end
    escape = if !@redshift_copy_base_options.include?('ESCAPE') && (json? || msgpack?)
               " ESCAPE"
             else
               ''
             end

    "copy #{@table_name_with_schema}#{copy_columns} from '%s' #{credentials} delimiter '#{@delimiter}' GZIP#{escape} #{@redshift_copy_base_options} #{@redshift_copy_options};"
  end

  def json?
    @file_type == 'json'
  end

  def msgpack?
    @file_type == 'msgpack'
  end

  def create_gz_file_from_flat_data(dst_file, chunk)
    gzw = nil
    begin
      gzw = Zlib::GzipWriter.new(dst_file)
      chunk.write_to(gzw)
    ensure
      gzw.close rescue nil if gzw
    end
    dst_file
  end

  def create_gz_file_from_structured_data(dst_file, chunk)
    redshift_table_columns = fetch_table_columns
    if redshift_table_columns == nil
      raise "failed to fetch the redshift table definition."
    elsif redshift_table_columns.empty?
      $log.warn format_log("no table on redshift or cannot access table. table_name=#{@table_name_with_schema}")
      return nil
    end

    if @redshift_copy_columns
      unknown_colmns = @redshift_copy_columns - redshift_table_columns
      unless unknown_colmns.empty?
        fail Fluent::ConfigError, "missing columns included in redshift_copy_columns - missing columns:\"#{unknown_colmns.join(',')}\""
      end
    end

    # convert json to tsv format text
    gzw = nil
    begin
      gzw = Zlib::GzipWriter.new(dst_file)
      chunk.msgpack_each do |record|
        next unless record
#        begin
          tsv_text = hash_to_table_text(record, redshift_table_columns)
          gzw.write(tsv_text) if tsv_text and not tsv_text.empty?
        # rescue => e
        #   text = record.is_a?(Hash) ? record[@record_log_tag] : record
        #   $log.error format_log("failed to create table text from #{@file_type}. text=(#{text})"), error:e.to_s
        #   $log.error_backtrace
        # end
      end
      return nil unless gzw.pos > 0
    ensure
      gzw.close rescue nil if gzw
    end
    dst_file
  end

  def fetch_table_columns
    @redshift_connection.exec(fetch_columns_sql) do |result|
      result.map { |row| row['column_name'] }
    end
  end

  def fetch_columns_sql
    sql = "select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '#{@redshift_tablename}'"
    sql << " and table_schema = '#{@redshift_schemaname}'" if @redshift_schemaname
    sql << " order by ordinal_position;"
    @last_sql = sql
    sql
  end

  def hash_to_table_text(hash, redshift_table_columns)
    if hash
      values = redshift_table_columns.map { |cn| hash[cn] }

      if values.compact.empty?
        $log.warn format_log("no data match for table columns on redshift. data=#{hash} table_columns=#{redshift_table_columns}")
        return ''
      else
        generate_line_with_delimiter(values, delimiter)
      end
    else
      ''
    end
  end

  def generate_line_with_delimiter(val_list, delimiter)
    val_list.collect do |val|
      case val
      when nil
        NULL_CHAR_FOR_COPY
      when ''
        ''
      when Hash, Array
        escape_text_for_copy(JSON.generate(val))
      else
        escape_text_for_copy(val.to_s)
      end
    end.join(delimiter) + "\n"
  end

  def escape_text_for_copy(val)
    val.gsub(/\\|\t|\n/, {"\\" => "\\\\", "\t" => "\\\t", "\n" => "\\\n"})  # escape tab, newline and backslash
  end

end

class RedshiftError < StandardError
  def initialize(msg)
    case msg
    when PG::Error
      @pg_error = msg
      super(msg.to_s)
      set_backtrace(msg.backtrace)
    else
      super
    end
  end

  attr_accessor :pg_error
end

class RedshiftConnection
  REDSHIFT_CONNECT_TIMEOUT = 10.0

  def initialize(db_conf)
    @db_conf = db_conf
    @connection = nil
  end

  attr_reader :db_conf

  def exec(sql, &block)
    conn = @connection
    conn = create_redshift_connection if conn.nil?
    if block
      conn.exec(sql) {|result| block.call(result)}
    else
      conn.exec(sql)
    end
  rescue PG::Error => e
    raise RedshiftError.new(e)
  ensure
    conn.close if conn && @connection.nil?
  end

  def connect_start
    @connection = create_redshift_connection
  end

  def close
    @connection.close rescue nil if @connection
    @connection = nil
  end

  private

  def create_redshift_connection
    conn = PG::Connection.connect_start(db_conf)
    raise RedshiftError.new("Unable to create a new connection.") unless conn
    if conn.status == PG::CONNECTION_BAD
      raise RedshiftError.new("Connection failed: %s" % [ conn.error_message ])
    end

    socket = conn.socket_io
    poll_status = PG::PGRES_POLLING_WRITING
    until poll_status == PG::PGRES_POLLING_OK || poll_status == PG::PGRES_POLLING_FAILED
      case poll_status
      when PG::PGRES_POLLING_READING
        IO.select([socket], nil, nil, REDSHIFT_CONNECT_TIMEOUT) or
          raise RedshiftError.new("Asynchronous connection timed out!(READING)")
      when PG::PGRES_POLLING_WRITING
        IO.select(nil, [socket], nil, REDSHIFT_CONNECT_TIMEOUT) or
          raise RedshiftError.new("Asynchronous connection timed out!(WRITING)")
      end
      poll_status = conn.connect_poll
    end

    unless conn.status == PG::CONNECTION_OK
      raise RedshiftError, ("Connect failed: %s" % [conn.error_message.to_s.lines.uniq.join(" ")])
    end

    conn
  rescue => e
    conn.close rescue nil if conn
    raise RedshiftError.new(e) if e.kind_of?(PG::Error)
    raise e
  end
end
