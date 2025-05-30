import boto3
import time

def create_emr_cluster():
    region = 'us-east-1'
    bucket = "weather-etl-data-st0263"

    emr = boto3.client('emr', region_name=region)
    response = emr.run_job_flow(
        Name="weather-etl-cluster",
        ReleaseLabel="emr-6.15.0",
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Livy'}
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Primary',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 32
                                },
                                'VolumesPerInstance': 2
                            }
                        ]
                    }
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 2,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 32
                                },
                                'VolumesPerInstance': 2
                            }
                        ]
                    }
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        Configurations=[
            {
                "Classification": "spark",
                "Properties": {
                    "maximizeResourceAllocation": "false"
                }
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.shuffle.service.enabled": "true",
                    "spark.memory.fraction": "0.6",
                    "spark.memory.storageFraction": "0.5",
                    "spark.sql.shuffle.partitions": "10",
                    "spark.default.parallelism": "10",
                    "spark.sql.files.maxPartitionBytes": "128m",
                    "spark.sql.autoBroadcastJoinThreshold": "10m",
                    "spark.driver.memory": "4g",
                    "spark.executor.memory": "4g",
                    "spark.executor.cores": "2",
                    "spark.executor.instances": "2",
                    "spark.driver.maxResultSize": "2g"
                }
            }
        ],
        Steps=[
            {
                'Name': 'ETL',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        f"s3://{bucket}/scripts/etl_script.py"
                    ]
                }
            },
            {
                'Name': 'Weather Analysis',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        f"s3://{bucket}/scripts/analysis_script.py"
                    ]
                }
            }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        LogUri=f"s3://{bucket}/logs/",
        VisibleToAllUsers=True,
        Tags=[
            {
                'Key': 'Project',
                'Value': 'WeatherETL'
            }
        ]
    )

    cluster_id = response['JobFlowId']
    print(f"✅ Cluster creado con ID: {cluster_id}")
    return cluster_id, bucket

def wait_for_cluster_completion(cluster_id, bucket):
    emr = boto3.client('emr', region_name='us-east-1')
    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        state = response['Cluster']['Status']['State']
        
        steps_response = emr.list_steps(ClusterId=cluster_id)
        steps = steps_response['Steps']
        
        print(f"\n📡 Estado actual del cluster: {state}")
        print("\nEstado de los steps:")
        for step in steps:
            step_state = step['Status']['State']
            step_name = step['Name']
            print(f"- {step_name}: {step_state}")
            
            if step_state == 'FAILED':
                print(f"  Error en {step_name}:")
                failure_details = step['Status'].get('FailureDetails', {})
                reason = failure_details.get('Reason', 'No reason provided')
                message = failure_details.get('Message', 'No message provided')
                print(f"  Razón: {reason}")
                print(f"  Mensaje: {message}")
                print(f"  Logs: s3://{bucket}/logs/{cluster_id}/steps/{step['Id']}/")
        
        if state in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
            print(f"\n⚠️ Cluster finalizó con estado: {state}")
            print(f"Revisa los logs completos en: s3://{bucket}/logs/{cluster_id}/")
            break
        time.sleep(30)

if __name__ == "__main__":
    cluster_id, bucket = create_emr_cluster()
    wait_for_cluster_completion(cluster_id, bucket)
