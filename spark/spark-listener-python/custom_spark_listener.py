from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

class PythonSparkListener:
    """
    A custom Spark Listener written in Python.
    This listener will print information about job, stage, and task events,
    and it is enhanced to detect and report failures.
    """
    def __init__(self):
        # Get the gateway singleton and cache the JobFailed class for later checks
        sc = SparkSession.builder.getOrCreate().sparkContext
        self._gateway = sc._gateway
        self.JobFailed = self._gateway.jvm.org.apache.spark.scheduler.JobFailed

    def onApplicationEnd(self, applicationEnd):
        print(f"INFO: Application ended. Time: {applicationEnd.time()}")

    def onJobStart(self, jobStart):
        print(f"INFO: Job started. Job ID: {jobStart.jobId()}, Stage IDs: {list(jobStart.stageIds())}")

    def onJobEnd(self, jobEnd):
        # Check if the job result is an instance of the JobFailed class
        if isinstance(jobEnd.jobResult(), self.JobFailed):
            # jobEnd.jobResult().exception() returns a Java exception object.
            # .toString() gives a good summary of the exception and its cause.
            exception_str = jobEnd.jobResult().exception().toString()
            print(f"ERROR: Job failed. Job ID: {jobEnd.jobId()}. Exception: {exception_str}")
        else:
            job_result = jobEnd.jobResult().toString()
            print(f"INFO: Job ended. Job ID: {jobEnd.jobId()}, Result: {job_result}")

    def onStageSubmitted(self, stageSubmitted):
        print(f"INFO: Stage submitted. Stage ID: {stageSubmitted.stageInfo().stageId()}")

    def onStageCompleted(self, stageCompleted):
        stage_info = stageCompleted.stageInfo()
        print(f"INFO: Stage completed. Stage ID: {stage_info.stageId()}")
        # Check if the stage has a failure reason. failureReason() returns an Option[String].
        if stage_info.failureReason().isDefined():
            print(f"ERROR: Stage {stage_info.stageId()} failed. Reason: {stage_info.failureReason().get()}")

        cpu_time = stage_info.taskMetrics().executorCpuTime()
        print(f"  -> Stage {stage_info.stageId()} took {cpu_time / 1e9} seconds of CPU time.")


    def onTaskStart(self, taskStart):
        print(f"  -> Task started. Task ID: {taskStart.taskInfo().taskId()}, Stage ID: {taskStart.stageId()}")

    def onTaskEnd(self, taskEnd):
        # The reason for task failure, if any.
        reason = taskEnd.reason().toString()
        metrics = taskEnd.taskMetrics()
        print(f"  -> Task ended. Task ID: {taskEnd.taskInfo().taskId()}, Reason: {reason}")
        
        # Any reason other than 'Success' indicates a problem.
        # Common failure reasons include 'ExecutorLostFailure', 'FetchFailed', 'ExceptionFailure'.
        if reason != "Success":
            print(f"ERROR: Task {taskEnd.taskInfo().taskId()} in stage {taskEnd.stageId()} failed. Reason: {reason}")

        if metrics:
            print(f"    -> Records Read: {metrics.inputMetrics().recordsRead()}")
            print(f"    -> Bytes Read: {metrics.inputMetrics().bytesRead()}")
            print(f"    -> Records Written: {metrics.outputMetrics().recordsWritten()}")
            print(f"    -> Bytes Written: {metrics.outputMetrics().bytesWritten()}")


    # The 'implements' attribute is a special instruction to py4j to
    # create a Java proxy object that implements the specified Java interface.
    class Java:
        implements = ["org.apache.spark.scheduler.SparkListener"]


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PythonSparkListenerExample") \
        .master("local[2]") \
        .getOrCreate()

    sc = spark.sparkContext

    py_listener = PythonSparkListener()
    
    java_listener = py_listener._gateway.jvm.org.apache.spark.api.python.PythonGatewayServer.getPythonListener(py_listener)
    sc._jsc.sc().addSparkListener(java_listener)

    print("\n" + "="*40)
    print("Spark Listener attached. Starting a job designed to fail.")
    print("="*40 + "\n")

    # This is a UDF that will fail for a specific input value, causing the job to fail.
    def failing_udf_impl(name):
        if name == "Bob":
            raise ValueError("Deliberate failure for testing purposes on name 'Bob'")
        return len(name)

    failing_udf = udf(failing_udf_impl, IntegerType())

    try:
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("Alice", 4)]
        df = spark.createDataFrame(data, ["name", "value"])

        # We apply the failing UDF, which will cause the job to throw an exception.
        result_df = df.withColumn("name_len", failing_udf(df["name"]))
        
        # The .collect() action will trigger the job and the failure.
        result_df.collect()

    except Exception as e:
        # The Python driver will also receive the exception.
        # We catch it here so the program can exit gracefully.
        print("\n" + "="*40)
        print("Caught expected exception in the driver script:")
        print(f"{type(e).__name__}: {e}")
        print("="*40 + "\n")
    finally:
        sc._jsc.sc().removeSparkListener(java_listener)
        spark.stop()
        print("\n" + "="*40)
        print("Spark job finished and session stopped.")
        print("="*40 + "\n")