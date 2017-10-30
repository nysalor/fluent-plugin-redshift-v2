$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require 'fluent/load'
require 'fluent/test'
require 'fluent/test/driver/output'
require 'fakes3/server'

require 'fluent/plugin/out_redshift_v2'

RSpec.configure do |config|
  config.before(:all) do
    Fluent::Test.setup

    pid = Process.fork do
      Dir.mktmpdir do |dir|
        def $stderr.write(*_args); end  # Suppress outputs
        FakeS3::Server.new('0.0.0.0', 12345, FakeS3::FileStore.new(dir, false), 'localhost', nil, nil).serve
      end
    end
    at_exit do
      Process.kill(:TERM, pid)
    end
  end
end
