from aws_lambda_powertools import Tracer

tracer = Tracer()

# for more info check https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html#model-parameters-anthropic-claude-messages-request-response
VALID_FILE_EXTENSIONS = [
    'jpeg',
    'jpg',
    'png',
    'webp',
    'gif',
]

@tracer.capture_method
def validate_file_extension(file_extension):
    if file_extension not in VALID_FILE_EXTENSIONS:
        raise ValueError(f"Invalid file extension. Supported file extensions are {', '.join(VALID_FILE_EXTENSIONS)}")


@tracer.capture_method
def get_claude_anthropic_media_type(file_extension):
    validate_file_extension(file_extension)
    if file_extension in ('jpg', 'jpeg'):
        return "image/jpeg"
    return f"image/{file_extension}"