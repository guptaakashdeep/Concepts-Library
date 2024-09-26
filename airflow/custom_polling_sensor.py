from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.utils.context import Context


class PollingSensor(BaseSensorOperator):
    template_fields = ('cmd')
    def __init__(self, *, cmd, **kwargs):
        self.cmd = cmd
        super().__init__(**kwargs)
    
    def poke(self, context: Context):
        try:
            polling_runner = BashOperator(task_id="polling_command",bash_command=self.cmd)
            polling_runner.execute(context)
            return True
        except AirflowException as e:
            print(e)
            return False
        except Exception as e:
            raise e