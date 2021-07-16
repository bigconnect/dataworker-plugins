package io.bigconnect.dw.tester;

import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.cmdline.CommandLineTool;
import com.mware.core.ingest.dataworker.DataWorkerMemoryTracer;
import com.mware.core.process.DataWorkerRunnerProcess;
import com.mware.core.status.model.QueueStatus;
import com.mware.core.status.model.Status;
import com.mware.core.status.model.Status.CounterMetric;
import com.mware.ge.Element;
import lombok.SneakyThrows;

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
                .newDocument("title",
                        "Șeful Organizației Mondiale a Sănătății, Tedros Adhanom Ghebreyesus, a recunoscut că excluderea ipotezei că noul coronavirus a scăpat dintr-un laborator chinez este prematură, adăugând că a cerut Beijingului să fie mai transparent față de cercetătorii care caută originea pandemiei de Sars-Cov-2.\n" +
                                "\n" +
                                "Într-o rară ieșire din obișnuita deferență față de marile puteri din OMS, directorul organizației a declarat că obținerea informațiilor primare a fost o provocare pentru echipa internațională care a mers în China anul acesta pentru a investiga sursa Covid-19, scrie The Guardian. Primul caz la oameni a fost identificat în Wuhan.\n" +
                                "\n" +
                                "Tedros a declarat presei că agenția pentru sănătate a ONU din Geneva ”cere actualmente Chinei să fie mai transparentă, mai deschisă și să coopereze, în special în privința informațiilor și datelor primare pe care le-am cerut în primele etape ale pandemiei”.\n" +
                                "\n" +
                                "El a spus că a existat o campanie ”prematură” pentru excluderea ipotezei că virusul ar fi putut scăpa dintr-un laborator al guvernului chinez din Wuhan, excludere ce a subminat raportul OMS din martie, ce a concluzionat că o scăpare de laborator este ”extrem de puțin probabilă”.")
                .push();

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
