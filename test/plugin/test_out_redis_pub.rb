require "helper"
require "fluent/plugin/out_redis_pub.rb"

class RedisPubOutputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  test "failure" do
    flunk
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::RedisPubOutput).configure(conf)
  end
end
