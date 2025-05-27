import boto3
import time
 
def create_emr_cluster():
    emr_client = boto3.client('emr', region_name='us-east-1')
    bucket = "weather-etl-data-st0263"
    response = emr_client.run_job_flow(
        Name='weather-etl-cluster',
        ReleaseLabel='emr-6.9.0',
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hive'},
            {'Name': 'Hue'},
            {'Name': 'JupyterHub'}
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        Steps=[
            {
                'Name': 'SimpleETLStep',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--conf', 'spark.executor.memory=2g',
                        f"s3://{bucket}/scripts/etl_script.py"
                    ]
                }
            }
        ],
        LogUri=f"s3://{bucket}/logs/",
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        Configurations=[
            {
                "Classification": "hue-ini",
                "Properties": {},
                "Configurations": [
                    {
                        "Classification": "desktop",
                        "Properties": {
                            "secret_signature": "your-secret-signature" 
                        }
                    }
                ]
            },
            {
                "Classification": "spark",
                "Properties": {
                    "maximizeResourceAllocation": "true"
                }
            }
        ],
        VisibleToAllUsers=True,
        BootstrapActions=[
            {
                'Name': 'Install Jupyter Libraries',
                'ScriptBootstrapAction': {
                    'Path': f"s3://{bucket}/scripts/bootstrap_jupyter.sh"
                }
            }
        ]
    )
    cluster_id = response['JobFlowId']
    print(f"Created EMR cluster: {cluster_id}")
    while True:
        cluster = emr_client.describe_cluster(ClusterId=cluster_id)
        state = cluster['Cluster']['Status']['State']
        if state in ['RUNNING', 'WAITING']:
            break
        elif state in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
            print(f"Cluster {cluster_id} failed with state: {state}")
            return
        time.sleep(30)
    print(f"Cluster {cluster_id} is ready")
    return cluster_id
 
if __name__ == "__main__":
    create_emr_cluster()