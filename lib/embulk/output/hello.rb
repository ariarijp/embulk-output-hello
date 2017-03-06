Embulk::JavaPlugin.register_output(
  "hello", "org.embulk.output.hello.HelloOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
