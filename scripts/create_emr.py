import boto3
import time

def create_emr_cluster():
    region = 'us-east-1'
    bucket = "weather-etl-data-st0263"

    emr = boto3.client('emr', region_name=region)
    response = emr.run_job_flow(
        Name="weather-etl-cluster-test",
        ReleaseLabel="emr-6.9.0",
        Applications=[{'Name': 'Spark'}],
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
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        Steps=[
            {
                'Name': 'Basic ETL Step',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        f"s3://{bucket}/scripts/basic_etl.py"
                    ]
                }
            }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        LogUri=f"s3://{bucket}/logs/",
        VisibleToAllUsers=True
    )

    cluster_id = response['JobFlowId']
    print(f"✅ Cluster creado con ID: {cluster_id}")
    return cluster_id, bucket

def wait_for_cluster_completion(cluster_id, bucket):
    emr = boto3.client('emr', region_name='us-east-1')
    while True:
        state = emr.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']
        print(f"📡 Estado actual del cluster: {state}")
        if state in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
            print(f"⚠️ Cluster finalizó con estado: {state}. Revisa logs en s3://{bucket}/logs/")
            break
        time.sleep(30)

if __name__ == "__main__":
    cluster_id, bucket = create_emr_cluster()
    wait_for_cluster_completion(cluster_id, bucket)
