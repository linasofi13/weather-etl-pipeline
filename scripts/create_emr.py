import boto3

def create_emr_cluster_quick_steps():
    emr = boto3.client('emr', region_name='us-east-1')
    bucket = "weather-etl-data-st0263"

    resp = emr.run_job_flow(
        Name="quick-steps-test",
        ReleaseLabel="emr-6.15.0",
        Applications=[{'Name': 'Spark'}],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',  # >= m5.xlarge para ML
                    'InstanceCount': 1
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        Steps=[
            {
                'Name': 'ETL Step',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--driver-memory', '4g',
                        '--executor-memory', '4g',
                        f"s3://{bucket}/scripts/etl_script.py"
                    ]
                }
            },
            {
                'Name': 'Analysis Step',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--driver-memory', '4g',
                        '--executor-memory', '4g',
                        '--packages', 'org.apache.spark:spark-mllib_2.12:3.5.1',
                        f"s3://{bucket}/scripts/analysis_script.py"
                    ]
                }
            }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        LogUri=f"s3://{bucket}/logs/",
        VisibleToAllUsers=True
    )

    cluster_id = resp['JobFlowId']
    print(f"Cluster con steps creado (se terminará al completar): {cluster_id}")
    return cluster_id

if __name__ == "__main__":
    create_emr_cluster_quick_steps()
