package io.bigconnect.dw.tester;

import com.mware.bigconnect.ffmpeg.VideoFormat;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.cmdline.CommandLineTool;
import com.mware.core.ingest.dataworker.DataWorkerMemoryTracer;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.process.DataWorkerRunnerProcess;
import com.mware.core.status.model.QueueStatus;
import com.mware.core.status.model.Status;
import com.mware.core.status.model.Status.CounterMetric;
import com.mware.ge.Element;
import lombok.SneakyThrows;

import java.io.FileInputStream;
import java.util.Map;

public class DataWorkerTester extends CommandLineTool {
    @Override
    protected int run() throws Exception {
        DataWorkerMemoryTracer.ENABLED = true;

        InjectHelper.getInstance(DataWorkerRunnerProcess.class);

        while (!workQueueRepository.hasDataWorkerRunner()) {
            LOGGER.info("Waiting for DataWorkers to start....");
            Thread.sleep(500);
        }

        Element e = EntityCreator.build(graph, workQueueRepository)
                .newVideo("video1", new FileInputStream("/home/flavius/public/dataworker-plugins/tester/input/test.mp4"))
                .setProperty(MediaBcSchema.MEDIA_VIDEO_FORMAT.getPropertyName(), VideoFormat.MP4.name())
                .push();

        waitForQueueEmpty();
        DataWorkerMemoryTracer.print(); DataWorkerMemoryTracer.clear();

        EntityCreator.build(graph, workQueueRepository)
                .with(e)
                .setProperty(RawObjectSchema.RAW_LANGUAGE.getPropertyName(), "ro")
                .push(RawObjectSchema.RAW_LANGUAGE.getPropertyName(), "");

        waitForQueueEmpty();
        DataWorkerMemoryTracer.print(); DataWorkerMemoryTracer.clear();

        Thread.sleep(Long.MAX_VALUE);
        return 0;
    }

    @SneakyThrows
    public void waitForQueueEmpty() {
        Thread.sleep(1000);
        boolean hasMessages = true;
        while (hasMessages) {
            Map<String, Status> queuesStatus = workQueueRepository.getQueuesStatus();
            QueueStatus status = (QueueStatus) queuesStatus.values().iterator().next();
            CounterMetric messages = (CounterMetric) status.getMetrics().get("messages");
            hasMessages = messages.getCount() > 0;
            Thread.sleep(100);
        }
    }

    public static void main(String[] args) throws Exception {
        CommandLineTool.main(new DataWorkerTester(), args);
    }
}
