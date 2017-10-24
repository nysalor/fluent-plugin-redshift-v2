require "spec_helper"

describe Fluent::Plugin::Redshift::V2 do
  it "has a version number" do
    expect(Fluent::Plugin::Redshift::V2::VERSION).not_to be nil
  end

  it "does something useful" do
    expect(false).to eq(true)
  end
end
