package org.embulk.output.hello;

import com.google.common.base.Optional;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.slf4j.Logger;

import java.util.List;

public class HelloOutputPlugin implements OutputPlugin {
    private final Logger log;

    public interface PluginTask extends Task {
        // configuration option 1 (required integer)
        @Config("option1")
        public int getOption1();

        // configuration option 2 (optional string, null is not allowed)
        @Config("option2")
        @ConfigDefault("\"myvalue\"")
        public String getOption2();

        // configuration option 3 (optional string, null is allowed)
        @Config("option3")
        @ConfigDefault("null")
        public Optional<String> getOption3();
    }

    public HelloOutputPlugin() {
        log = Exec.getLogger(getClass());
        log.info("HelloOutputPlugin");
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  OutputPlugin.Control control) {
        log.info("HelloOutputPlugin.transaction");
        PluginTask task = config.loadConfig(PluginTask.class);

        log.info("{}: {}", "option1", task.getOption1());
        log.info("{}: {}", "option2", task.getOption2());
        log.info("{}: {}", "option3", task.getOption3());

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             OutputPlugin.Control control) {
        log.info("HelloOutputPlugin.resume");
        throw new UnsupportedOperationException("hello output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports) {
        log.info("HelloOutputPlugin.cleanup");
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource,
                                        Schema schema,
                                        int taskIndex) {
        log.info("HelloOutputPlugin.open");
        PluginTask task = taskSource.loadTask(PluginTask.class);

        HelloPageOutput pageOutput = new HelloPageOutput();
        pageOutput.open(schema);
        return pageOutput;
    }

    public class HelloPageOutput implements TransactionalPageOutput {
        private PageReader pageReader;

        void open(final Schema schema) {
            log.info("HelloPageOutput.open");
            pageReader = new PageReader(schema);
        }

        @Override
        public void add(Page page) {
            log.info("HelloPageOutput.add");

            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                pageReader.getSchema().visitColumns(new ColumnVisitor() {
                    @Override
                    public void booleanColumn(Column column) {
                        log.info("{}: {}", column.getName(), pageReader.getBoolean(column));
                    }

                    @Override
                    public void longColumn(Column column) {
                        log.info("{}: {}", column.getName(), pageReader.getLong(column));
                    }

                    @Override
                    public void doubleColumn(Column column) {
                        log.info("{}: {}", column.getName(), pageReader.getDouble(column));
                    }

                    @Override
                    public void stringColumn(Column column) {
                        log.info("{}: {}", column.getName(), pageReader.getString(column));
                    }

                    @Override
                    public void timestampColumn(Column column) {
                        log.info("{}: {}", column.getName(), pageReader.getTimestamp(column));
                    }

                    @Override
                    public void jsonColumn(Column column) {
                        log.info("{}: {}", column.getName(), pageReader.getJson(column).toJson());
                    }
                });
            }
        }

        @Override
        public void finish() {
            log.info("HelloPageOutput.finish");
        }

        @Override
        public void close() {
            log.info("HelloPageOutput.close");
        }

        @Override
        public void abort() {
            log.info("HelloPageOutput.abort");
        }

        @Override
        public TaskReport commit() {
            log.info("HelloPageOutput.commit");
            return null;
        }
    }
}
