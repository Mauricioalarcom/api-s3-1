import json
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    """
    Función Lambda para crear un nuevo directorio (prefijo) en un bucket S3 existente
    Espera un body JSON con los campos 'bucket_name' y 'directory_name'
    Nota: En S3 los directorios son virtuales y se crean al subir un objeto con un prefijo
    """
    try:
        # Parsear el body del request
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})
        
        bucket_name = body.get('bucket_name')
        directory_name = body.get('directory_name')
        
        if not bucket_name or not directory_name:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Los parámetros bucket_name y directory_name son requeridos'
                })
            }
        
        # Asegurar que el nombre del directorio termine con /
        if not directory_name.endswith('/'):
            directory_name += '/'
        
        # Crear cliente de S3
        s3_client = boto3.client('s3')
        
        # Verificar que el bucket existe
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'El bucket {bucket_name} no existe o no tienes acceso a él'
                })
            }
        
        # Crear el directorio (en S3 se crea un objeto vacío con el prefijo terminado en /)
        s3_client.put_object(Bucket=bucket_name, Key=directory_name)
        
        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': f'Directorio {directory_name} creado exitosamente en el bucket {bucket_name}',
                'bucket_name': bucket_name,
                'directory_name': directory_name
            })
        }
        
    except ClientError as e:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'Error al crear el directorio: {str(e)}'
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
