import json
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    """
    Función Lambda para crear un nuevo bucket en S3
    Espera un body JSON con el campo 'bucket_name'
    """
    try:
        # Parsear el body del request
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})
        
        bucket_name = body.get('bucket_name')
        
        if not bucket_name:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'El parámetro bucket_name es requerido'
                })
            }
        
        # Crear cliente de S3
        s3_client = boto3.client('s3')
        
        # Crear el bucket
        s3_client.create_bucket(Bucket=bucket_name)
        
        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': f'Bucket {bucket_name} creado exitosamente',
                'bucket_name': bucket_name
            })
        }
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyExists':
            message = f'El bucket {bucket_name} ya existe'
        elif error_code == 'BucketAlreadyOwnedByYou':
            message = f'Ya eres dueño del bucket {bucket_name}'
        else:
            message = f'Error al crear el bucket: {str(e)}'
        
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': message
            })
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'Error interno: {str(e)}'
            })
        }
