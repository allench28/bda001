import os
import random
import string
from aws_cdk import (
    Stack,
    CfnOutput,
    Tags,
    aws_bedrock as bedrock,
    aws_iam as iam,
)
from constructs import Construct

PROJECT_NAME = os.environ.get("PROJECT_NAME", "LITE_DEMO")
ENV_NAME = os.environ.get("ENV", "dev")

def _kebab(s: str) -> str:
    return s.lower().replace("_", "-").replace(" ", "-")

class LiteDemoBDAProjectStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, s3_stack=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Service role untuk BDA (WAJIB dioper ke project) ---
        bda_role = iam.Role(
            self,
            "BDARole",
            role_name=f"{PROJECT_NAME}-BDA-Role-{ENV_NAME}",
            assumed_by=iam.ServicePrincipal("bedrock.amazonaws.com"),
            description="Service role for Bedrock Data Automation project",
        )

        if s3_stack:
            bda_role.add_to_policy(
                iam.PolicyStatement(
                    actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                    resources=[
                        f"arn:aws:s3:::{s3_stack.bucket_name}",
                        f"arn:aws:s3:::{s3_stack.bucket_name}/*",
                    ],
                )
            )

        bda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=["arn:aws:bedrock:*::foundation-model/*"],
            )
        )

        # --- Nama project ---
        rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        project_name = f"{_kebab(PROJECT_NAME)}-bda-{ENV_NAME}"

        # --- Standard Output Configuration (mengikuti struktur docs) ---
        # Fokus: Document modality saja, output Markdown, tanpa CSV/Generative/BBox.
        standard_output_config = bedrock.CfnDataAutomationProject.StandardOutputConfigurationProperty(
            document=bedrock.CfnDataAutomationProject.DocumentStandardOutputConfigurationProperty(
                extraction=bedrock.CfnDataAutomationProject.DocumentStandardExtractionProperty(
                    granularity=bedrock.CfnDataAutomationProject.DocumentExtractionGranularityProperty(
                        # Aktifkan hanya Page + Element (sesuai UI-mu)
                        types=["PAGE", "ELEMENT"]  # opsi lain: "DOCUMENT", "WORD"
                    ),
                    bounding_box=bedrock.CfnDataAutomationProject.DocumentBoundingBoxProperty(
                        state="DISABLED"
                    ),
                ),
                generative_field=bedrock.CfnDataAutomationProject.DocumentStandardGenerativeFieldProperty(
                    state="DISABLED"
                ),
                output_format=bedrock.CfnDataAutomationProject.DocumentOutputFormatProperty(
                    text_format=bedrock.CfnDataAutomationProject.DocumentOutputTextFormatProperty(
                        types=["MARKDOWN"]
                    ),
                    additional_file_format=bedrock.CfnDataAutomationProject.DocumentOutputAdditionalFileFormatProperty(
                        state="DISABLED"  # tidak perlu CSV/JSON+
                    ),
                ),
            )
        )

        # --- Create Project ---
        bda_project = bedrock.CfnDataAutomationProject(
            self,
            "BDAProject",
            project_name=project_name,
            # service_role_arn=bda_role.role_arn,  # penting!
            standard_output_configuration=standard_output_config,
        )

        # Tag & outputs
        Tags.of(self).add("Project", PROJECT_NAME)
        Tags.of(self).add("Environment", ENV_NAME)

        CfnOutput(self, "BDAProjectArn", value=bda_project.attr_project_arn)
        CfnOutput(self, "BDAProjectName", value=project_name)
        CfnOutput(self, "BDARoleArn", value=bda_role.role_arn)

        self.bda_project = bda_project
        self.bda_role = bda_role
        self.project_arn = bda_project.attr_project_arn