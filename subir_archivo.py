import json
import base64
import boto3
from botocore.exceptions import ClientError

def upload_base_64_to_s3(s3_bucket_name, s3_file_name, base_64_str):
    """
    Allows for the upload of a base64 string to a s3 object, may need fleshing out down the line, returns location
    of file in S3
    :param s3_bucket_name: S3 bucket name to push image to
    :param s3_file_name: File name
    :param base_64_str: base 64 string of the image to push to S3
    :return: Tuple of bucket_name and s3_file_name
    """
    s3 = boto3.resource('s3')
    s3.Object(s3_bucket_name, s3_file_name).put(Body=base64.b64decode(base_64_str))
    return (s3_bucket_name, s3_file_name)

def lambda_handler(event, context):
    """
    Función Lambda para subir un archivo codificado en base64 a un directorio en un bucket S3
    Espera un body JSON con los campos:
    - 'bucket_name': nombre del bucket
    - 'directory_name': nombre del directorio (opcional, puede ser vacío)
    - 'file_name': nombre del archivo
    - 'file_content': contenido del archivo codificado en base64
    """
    try:
        # Parsear el body del request
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})
        
        bucket_name = body.get('bucket_name')
        directory_name = body.get('directory_name', '')
        file_name = body.get('file_name')
        file_content = body.get('file_content')
        
        if not bucket_name or not file_name or not file_content:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Los parámetros bucket_name, file_name y file_content son requeridos'
                })
            }
        
        # Construir la ruta completa del archivo
        if directory_name:
            # Asegurar que el directorio termine con /
            if not directory_name.endswith('/'):
                directory_name += '/'
            s3_file_path = f"{directory_name}{file_name}"
        else:
            s3_file_path = file_name
        
        # Verificar que el bucket existe
        s3_client = boto3.client('s3')
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
        
        # Subir el archivo usando la función proporcionada
        try:
            result = upload_base_64_to_s3(bucket_name, s3_file_path, file_content)
            
            return {
                'statusCode': 201,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'message': f'Archivo subido exitosamente',
                    'bucket_name': result[0],
                    'file_path': result[1],
                    's3_url': f's3://{result[0]}/{result[1]}'
                })
            }
        except Exception as e:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'Error al decodificar o subir el archivo: {str(e)}'
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
                'error': f'Error al subir el archivo: {str(e)}'
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
