# Deployment Guide

## For End Users (Simple Deployment)

### Step 1: Deploy EC2 Instance
```bash
aws cloudformation create-stack \
  --stack-name cdk-deployment-instance \
  --template-body file://ec2-deployment.yaml \
  --parameters ParameterKey=KeyPairName,ParameterValue=your-key-name \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

Wait for stack creation (~5 minutes):
```bash
aws cloudformation wait stack-create-complete \
  --stack-name cdk-deployment-instance \
  --region us-east-1
```

### Step 2: Get Instance IP
```bash
aws cloudformation describe-stacks \
  --stack-name cdk-deployment-instance \
  --query 'Stacks[0].Outputs' \
  --region us-east-1
```

### Step 3: SSH and Deploy
```bash
ssh -i your-key.pem ec2-user@<PUBLIC_IP>

# Once connected, simply run:
./deploy.sh
```

### Step 4: Cleanup (When Done)
```bash
# On EC2 instance:
./cleanup.sh

# Then delete the EC2 stack:
aws cloudformation delete-stack \
  --stack-name cdk-deployment-instance \
  --region us-east-1
```

---

## Why This Approach is Better Than AMI

### ✅ Advantages:
1. **No AMI management** - Works in all regions automatically
2. **Always up-to-date** - Pulls latest code from GitHub
3. **No manual steps** - Everything automated via UserData
4. **IAM role included** - No credential management needed
5. **Cost effective** - Only pay for EC2 runtime
6. **Easy updates** - Just update CloudFormation template

### ❌ Your Original AMI Approach Issues:
1. AMI is region-specific (need to copy to each region)
2. AMI becomes outdated (need to rebuild for updates)
3. Manual setup still required
4. Larger storage costs
5. More complex to maintain

---

## Architecture

```
User → CloudFormation Stack → EC2 Instance (with IAM Role)
                                    ↓
                              UserData Script
                                    ↓
                         1. Install dependencies
                         2. Clone GitHub repo
                         3. Setup Python venv
                         4. Create deploy.sh
                                    ↓
                         User runs: ./deploy.sh
                                    ↓
                         CDK deploys all stacks
```

---

## Required Permissions

The CloudFormation template creates an IAM role with:
- **PowerUserAccess** - For creating AWS resources
- **IAM permissions** - For creating Lambda execution roles

This is the **minimum required** for CDK deployment.

---

## Cost Estimate

- **EC2 t3.medium**: ~$0.04/hour (~$30/month if left running)
- **Recommendation**: Terminate EC2 after deployment
- **Total deployment time**: ~10-15 minutes
- **Cost per deployment**: ~$0.01

---

## Troubleshooting

### Check UserData logs:
```bash
ssh ec2-user@<IP>
sudo cat /var/log/user-data.log
```

### Manual deployment:
```bash
cd /home/ec2-user/bda001
source .venv/bin/activate
./deploy_auto.sh
```

### Check IAM role:
```bash
aws sts get-caller-identity
```
