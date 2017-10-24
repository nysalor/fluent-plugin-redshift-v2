class Fluent::RedshiftOutputV2 < Fluent::BufferedOutput
  Fluent::Plugin.register_output('redshift_v2', self)
end
