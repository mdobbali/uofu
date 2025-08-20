from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.vcs import Github
from diagrams.aws.devtools import Codepipeline, Codebuild
from diagrams.aws.storage import S3
from diagrams.aws.management import Cloudformation, Cloudwatch
from diagrams.aws.compute import Lambda
from diagrams.aws.mobile import APIGateway
from diagrams.aws.integration import Eventbridge, SNS
from diagrams.aws.management import SystemsManager
from diagrams.aws.security import KMS

L = {
    1: "1  Push/Merge",
    2: "2  Source → artifact",
    3: "3  Build&Test pulls artifact",
    4: "4  Package → artifact(s)",
    5: "5  Deploy (env)",
    6: "6  Smoke test",
    7: "7  Approval → Deploy prod",
    8: "8  Alerts on failure"
}

with Diagram("CI/CD (AWS) — Lambda path", filename="cicd_lambda", show=False, graph_attr={"rankdir": "LR"}):

    with Cluster("Developer"):
        gh = Github("GitHub Repo")

    with Cluster("AWS"):
        cp = Codepipeline("CodePipeline")
        s3 = S3("S3 (artifacts, versioned, private)")
        cb_build = Codebuild("CodeBuild: Build & Test")

        ssm = SystemsManager("SSM Params + Secrets Manager")
        kms = KMS("KMS")

        with Cluster("Environments"):
            with Cluster("dev"):
                cb_deploy_dev = Codebuild("CodeBuild: Deploy (dev)")
                cfn_dev = Cloudformation("SAM/CFN Stack (dev)")
                lam_dev = Lambda("Lambda (alias: dev)")
                api_dev = APIGateway("API Gateway (dev)")

            with Cluster("stage"):
                cb_deploy_stage = Codebuild("CodeBuild: Deploy (stage)")
                cfn_stage = Cloudformation("SAM/CFN Stack (stage)")
                lam_stage = Lambda("Lambda (alias: stage)")
                api_stage = APIGateway("API Gateway (stage)")

            with Cluster("prod"):
                # prod uses canary/alias shift
                cb_deploy_prod = Codebuild("CodeBuild: Deploy (prod)")
                cfn_prod = Cloudformation("SAM/CFN Stack (prod)")
                lam_prod = Lambda("Lambda (alias: prod)")
                api_prod = APIGateway("API Gateway (prod)")

        cw = Cloudwatch("CloudWatch Logs & Metrics")
        eb = Eventbridge("EventBridge (failures)")
        sns = SNS("SNS → Slack/Email")

    # 1: developer triggers pipeline
    gh >> Edge(label=L[1]) >> cp

    # 2: source → artifact
    cp >> Edge(label=L[2]) >> s3

    # 3: build&test pulls source artifact
    s3 >> Edge(label=L[3]) >> cb_build
    cb_build << Edge(style="dashed", label="read minimal") << ssm
    ssm << Edge(style="dashed") << kms

    # 4: package outputs back to artifacts
    cb_build >> Edge(label=L[4]) >> s3

    # 5: deploy to dev
    s3 >> Edge(label=L[5].replace("(env)", "(dev)")) >> cb_deploy_dev >> cfn_dev >> lam_dev >> api_dev
    # 6: smoke test (dev)
    api_dev >> Edge(label=L[6]) >> cw

    # 5: deploy to stage
    s3 >> Edge(label=L[5].replace("(env)", "(stage)")) >> cb_deploy_stage >> cfn_stage >> lam_stage >> api_stage

    # 7: approval then deploy to prod with canary/alias
    cp >> Edge(label=L[7]) >> cb_deploy_prod >> cfn_prod >> lam_prod >> api_prod
    lam_prod >> Edge(label="canary/alias") >> cw

    # 8: alerts on failure
    for n in [cp, cb_build, cb_deploy_dev, cb_deploy_stage, cb_deploy_prod, cfn_dev, cfn_stage, cfn_prod, lam_dev, lam_stage, lam_prod]:
        n >> Edge(style="dashed", label=L[8]) >> eb >> sns