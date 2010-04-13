# attempts.rb
# this example shows you how to use the exception limiting feature of beetle
# it allows you to control the number of retries your handler will go through
#
# ! check the examples/README for information on starting your redis/rabbit !
#
# start it with ruby attempts.rb

require "rubygems"
require File.expand_path(File.dirname(__FILE__)+"/../lib/beetle")

# set Beetle log level to info, noisy but great for testing
Beetle.config.logger.level = Logger::INFO

# setup client
client = Beetle::Client.new
client.register_queue(:test)
client.register_message(:test)

# purge the test queue
client.purge(:test)

# empty the dedup store
client.deduplication_store.flushdb

# we're starting with 0 exceptions and expect our handler to process the message until the exception count has reached 10
$exceptions = 0
$max_exceptions = 10

# this is our message handler, it's wired to the message and will process the message 
class Handler < Beetle::Handler
  
  # called when the handler receives the message - fail everytime
  def process
    raise "failed #{$exceptions += 1} times"
  end
  
  # called when handler process raised an exception
  def error(exception)
    logger.info "execution failed: #{exception}"
  end
  
  # called when the handler has finally failed
  # we're stopping the event loop so this script stops after that
  def failure(result)
    super
    EM.stop_event_loop
  end
end

# register our handler to the message, configure it to our max_exceptions limit, we configure a delay of 0 to have it not wait before retrying
client.register_handler(:test, Handler, :exceptions => $max_exceptions, :delay => 0)

# publish a our test message
client.publish(:test, "snafu")

# and wait...
client.listen

# error handling, if everything went right this shouldn't happen.
if $exceptions != $max_exceptions + 1
  raise "something is fishy. Failed #{$exceptions} times"
end