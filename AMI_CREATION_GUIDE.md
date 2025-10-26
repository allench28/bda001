# AMI Creation Guide

## Step 1: Launch EC2 with CloudFormation

```bash
aws cloudformation create-stack \
  --stack-name cdk-base-instance \
  --template-body file://ec2-deployment.yaml \
  --parameters ParameterKey=KeyPairName,ParameterValue=your-key-name \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1

# Wait for completion
aws cloudformation wait stack-create-complete \
  --stack-name cdk-base-instance \
  --region us-east-1
```

## Step 2: Get Instance IP

```bash
PUBLIC_IP=$(aws cloudformation describe-stacks \
  --stack-name cdk-base-instance \
  --query 'Stacks[0].Outputs[?OutputKey==`PublicIP`].OutputValue' \
  --output text \
  --region us-east-1)

echo "Instance IP: $PUBLIC_IP"
```

## Step 3: SSH and Clone Repository

```bash
ssh -i your-key.pem ec2-user@$PUBLIC_IP

# Once connected:
cd /home/ec2-user
git clone https://github.com/allench28/bda001.git
cd bda001

# Verify packages are installed
node --version
python3 --version
cdk --version
aws --version

# Exit
exit
```

## Step 4: Create AMI

```bash
# Get Instance ID
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name cdk-base-instance \
  --query 'Stacks[0].Outputs[?OutputKey==`InstanceId`].OutputValue' \
  --output text \
  --region us-east-1)

# Create AMI
AMI_ID=$(aws ec2 create-image \
  --instance-id $INSTANCE_ID \
  --name "CDK-Deployment-Base-$(date +%Y%m%d-%H%M%S)" \
  --description "Base AMI with CDK tools and bda001 repository" \
  --region us-east-1 \
  --query 'ImageId' \
  --output text)

echo "AMI ID: $AMI_ID"

# Wait for AMI to be available
aws ec2 wait image-available --image-ids $AMI_ID --region us-east-1
echo "AMI is ready!"
```

## Step 5: Make AMI Public (Optional)

```bash
aws ec2 modify-image-attribute \
  --image-id $AMI_ID \
  --launch-permission "Add=[{Group=all}]" \
  --region us-east-1

echo "AMI is now public: $AMI_ID"
```

## Step 6: Update CloudFormation Template

Update `ec2-deployment.yaml` to use your custom AMI:

```yaml
Parameters:
  LatestAmiId:
    Type: String
    Default: ami-xxxxxxxxxxxxxxxxx  # Your AMI ID
    Description: Custom AMI with CDK tools pre-installed
```

## Step 7: Cleanup Base Instance

```bash
aws cloudformation delete-stack \
  --stack-name cdk-base-instance \
  --region us-east-1
```

---

## For End Users - Using Your Custom AMI

### Deploy with Custom AMI:

```bash
aws cloudformation create-stack \
  --stack-name my-cdk-deployment \
  --template-body file://ec2-deployment.yaml \
  --parameters \
    ParameterKey=KeyPairName,ParameterValue=my-key \
    ParameterKey=LatestAmiId,ParameterValue=ami-xxxxxxxxxxxxxxxxx \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

### SSH and Deploy:

```bash
ssh ec2-user@<PUBLIC_IP>
cd bda001
./deploy_auto.sh
```

---

## What's Included in the AMI

✅ **Pre-installed:**
- Node.js 18
- Python 3
- AWS CDK CLI
- AWS CLI v2
- Git
- Your repository code at `/home/ec2-user/bda001`

❌ **Not included (created on first run):**
- Python virtual environment (`.venv`)
- Python dependencies
- CDK bootstrap

These are created automatically when running `./deploy_auto.sh`

---

## Benefits of This Approach

1. **Fast deployment** - No package installation needed
2. **Consistent environment** - Same base for all users
3. **Offline capable** - Code already on instance
4. **Version control** - AMI captures specific code version

---

## Updating the AMI

When you update your code:

1. Launch instance from old AMI
2. Update code: `cd bda001 && git pull`
3. Create new AMI
4. Update CloudFormation template with new AMI ID
5. Distribute new AMI ID to users
