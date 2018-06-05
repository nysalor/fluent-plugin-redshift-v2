require "spec_helper"

describe Fluent::Plugin::RedshiftOutputV2 do
  let(:driver)   { Fluent::Test::Driver::Output.new(Fluent::Plugin::RedshiftOutputV2).configure(config) }
  let(:instance) { driver.instance }
  let(:aws_key_id) { 20.times.map { ('a'..'z').to_a.sample }.join }
  let(:aws_sec_key) { 40.times.map { ('a'..'z').to_a.sample }.join }
  let(:s3_region) { 'fakes3' }
  let(:s3_endpoint) { 'http://localhost:12345/' }
  let(:s3_bucket) { 'fluent-plugin-redshift-v2-test' }
  let(:columns) { %w(user_id remote_addr action version) }
  let(:s3_client) { Aws::S3::Client.new(access_key_id: aws_key_id, secret_access_key: aws_sec_key, region: s3_region, endpoint: s3_endpoint, force_path_style: true ) }
  let(:record) {
    {
      user_id: rand(1000),
      remote_addr: '1.2.3.4',
      action: 'run',
      version: '1.0'
    }
  }

  let(:config) do
    %[
       aws_key_id #{aws_key_id}
       aws_sec_key #{aws_sec_key}
       s3_bucket #{s3_bucket}
       s3_endpoint #{s3_endpoint}
       s3_region #{s3_region}
       path logs/plugin_test
       retry_wait 30s
       retry_limit 5
       flush_interval 30s
       flush_at_shutdown true
       buffer_chunk_limit 8m
       buffer_queue_limit 1280
       buffer_type memory
       redshift_host localhost
       redshift_dbname test
       redshift_user root
       redshift_password password
       redshift_tablename plugin_test
       record_log_tag test.metrics
       file_type json
      ]
  end

  describe "config" do
    it "should get aws_key_id" do
      expect(instance.aws_key_id).to eq(aws_key_id)
    end

    it "should get aws_sec_key" do
      expect(instance.aws_sec_key).to eq(aws_sec_key)
    end

    describe "#write" do
      before do
        instance.stub(:fetch_table_columns).and_return(columns)
        RedshiftConnection.any_instance.stub(:exec).and_return(true)
      end

      it "exec_copy should be called" do
        driver.run(default_tag: "test.metrics") { driver.feed(record) }
        expect(driver.events).not_to be_nil
      end

      it "exec_copy should create gz file" do
        driver.run(default_tag: "test.metrics") { driver.feed(record) }
        expect { s3_client.head_object(bucket: s3_bucket, key: instance.last_gz_path) }.not_to raise_error
      end

      it "exec_copy should create exact gz file" do
        driver.run(default_tag: "test.metrics") { driver.feed(record) }
        f = s3_client.get_object(bucket: s3_bucket, key: instance.last_gz_path)
        gz = Zlib::GzipReader.new(f.body)
        expect { gz.read }.not_to raise_error
      end

      it "copy_sql should returns actual sql" do
        driver.run(default_tag: "test.metrics") { driver.feed(record) }
          expect(instance.last_sql).to eq("copy plugin_test from 's3://#{s3_bucket}/#{instance.last_gz_path}' CREDENTIALS 'aws_access_key_id=#{aws_key_id};aws_secret_access_key=#{aws_sec_key}' delimiter '\t' GZIP ESCAPE FILLRECORD ACCEPTANYDATE TRUNCATECOLUMNS ;")
      end
    end
  end
end
