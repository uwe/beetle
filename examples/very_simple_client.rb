require "rubygems"
require File.expand_path("../lib/beetle", File.dirname(__FILE__))

# set Beetle log level to info, less noisy than debug
Beetle.config.logger.level = Logger::INFO

# setup client
client = Beetle::SimpleClient.new
client.register_message(:a_message)
client.register_message(:another_message)
client.register_message(:unimportant_message)
client.register_handler("greedy handler", :a_message, :another_message, :unimportant_message) {|message| puts "greedy handler got message: #{message.data}"}
client.register_handler("modest handler", :a_message) {|message| puts "modest handler got message: #{message.data}"}

# That's not part of the setup, just some cleanup
client.deduplication_store.flushdb
client.purge("greedy handler")
client.purge("modest handler")

# publish our message
client.publish(:a_message, 'a message')
client.publish(:another_message, 'another message')
client.publish(:unimportant_message, 'peter paul and marry are sitting in the kitchen')

# start listening
# this starts the event machine event loop using EM.run
# the block passed to listen will be yielded as the last step of the setup process
client.listen do
  EM.add_timer(0.1) { client.stop_listening }
end

