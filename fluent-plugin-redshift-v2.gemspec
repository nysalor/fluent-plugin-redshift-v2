# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-redshift-v2"
  spec.version       = "0.1.1"
  spec.authors       = ["Jun Yokoyama"]
  spec.email         = ["jun@larus.org"]

  spec.description   = %q{Amazon Redshift output plugin for Fluentd (inspired by fluent-plugin-redshift)}
  spec.summary       = spec.description
  spec.homepage      = "https://github.com/nysalor/fluent-plugin-redshift-v2"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.13"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "fakes3"
  spec.add_dependency "fluentd"
  spec.add_dependency "aws-sdk"
  spec.add_dependency "pg"
end
