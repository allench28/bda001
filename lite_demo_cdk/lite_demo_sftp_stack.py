from aws_cdk import (
    Stack,
    SecretValue,
    aws_transfer as transfer,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_s3 as s3,
)
from constructs import Construct
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

class LiteDemoSftpStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, s3_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---------------------- SFTP KeyPair ----------------------
        # Generate RSA keypair
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        
        # Get private key in PEM format
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode('utf-8')
        
        # Get public key in SSH format
        public_key = private_key.public_key()
        ssh_public_key = public_key.public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH
        ).decode('utf-8')

        # ---------------------- S3 Bucket ----------------------
        s3_bucket = s3_stack.bucket
        s3_bucket_name = s3_stack.bucket_name

        # ---------------------- IAM ----------------------
        # IAM role for Transfer Family server
        transfer_role = iam.Role(
            self, "TransferRole",
            assumed_by=iam.ServicePrincipal("transfer.amazonaws.com"),
            role_name=f"AWSTransferFamilySFTPUser-{self.region}"
        )
        
        # Add S3 permissions to the role
        s3_bucket.grant_read_write(transfer_role)

        # ---------------------- Transfer Family ----------------------
        # Transfer Family server
        sftp_server = transfer.CfnServer(
            self, "TransferServer",
            identity_provider_type="SERVICE_MANAGED",
            protocols=["SFTP"]
        )

        # Transfer Family user
        sftp_user = transfer.CfnUser(
            self, "TransferUser",
            server_id=sftp_server.attr_server_id,
            user_name="sftp-user",
            role=transfer_role.role_arn,
            ssh_public_keys=[ssh_public_key],
            home_directory=f"/{s3_bucket_name}/master_files/"
        )

        # ---------------------- Output ----------------------
        # Store private key in Secrets Manager
        secretsmanager.Secret(
            self, "PrivateKeySecret",
            secret_name="transfer/private-key",
            secret_string_value=SecretValue.unsafe_plain_text(private_pem)
        )