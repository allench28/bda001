from aws_cdk import (
    Stack,
    Duration,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_cloudwatch as cloudwatch_,
    aws_cloudwatch_actions as cloudwatch_actions_,
    aws_xray as xray,
    Tags
)
from constructs import Construct
from aap_backend_cdk.environment import *

class AapCloudWatchStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')


        # Create xray group
        XRayGroup = xray.CfnGroup(self, 'lambda_error', filter_expression='annotation.lambda_error = "true"', group_name='lambda_error')

        # Create sns topic for cloudwatch alarm
        LambdaAlarmTopic = sns.Topic(
            self, 'LambdaAlarmTopic', 
            display_name="Aap-LambdaAlarm-{}".format(env), 
            topic_name="Aap-LambdaAlarm-{}".format(env),
        )

        # Create CloudWatch alarm
        LambdaErrorsAlarm = cloudwatch_.Alarm(
            self, 'LambdaErrorsAlarm',
            metric=cloudwatch_.Metric(
                namespace='AWS/X-Ray',
                metric_name='ApproximateTraceCount',
                dimensions_map={ 'GroupName': XRayGroup.group_name},
                period=Duration.minutes(1),
                statistic=cloudwatch_.Stats.SUM,
            ),
            evaluation_periods=1,
            threshold=1.0,
            actions_enabled=True,
            alarm_name="Aap-LambdaErrorsAlarm-{}".format(env.upper())
        )

        LambdaErrorsAlarm.add_alarm_action(cloudwatch_actions_.SnsAction(LambdaAlarmTopic))