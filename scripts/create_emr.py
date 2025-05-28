import boto3
import time

def create_emr_cluster():
    emr = boto3.client('emr', region_name='us-east-1')
    bucket = "weather-etl-data-st0263"

    response = emr.run_job_flow(
        Name="weather-etl-cluster",
        ReleaseLabel="emr-6.9.0",
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
            'KeepJobFlowAliveWhenNoSteps': False,  # Cierra al terminar el paso
            'TerminationProtected': False
        },
        Steps=[
            {
                'Name': 'Weather ETL Step',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
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
        BootstrapActions=[
            {
                'Name': 'Install Jupyter Libraries',
                'ScriptBootstrapAction': {
                    'Path': f"s3://{bucket}/scripts/bootstrap_jupyter.sh"
                }
            }
        ],
        VisibleToAllUsers=True
    )

    cluster_id = response['JobFlowId']
    print(f"Cluster creado con ID: {cluster_id}")
    return cluster_id, bucket

def wait_for_cluster_completion(cluster_id, bucket):
    emr = boto3.client('emr', region_name='us-east-1')
    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        state = response['Cluster']['Status']['State']
        print(f"Estado actual del cluster: {state}")
        if state in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
            print(f"Cluster finalizó con estado: {state}. Revisa logs en s3://{bucket}/logs/")
            break
        time.sleep(30)

if __name__ == "__main__":
    cluster_id, bucket = create_emr_cluster()
    wait_for_cluster_completion(cluster_id, bucket)
