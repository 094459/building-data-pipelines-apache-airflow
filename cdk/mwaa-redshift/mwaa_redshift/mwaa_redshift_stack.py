from aws_cdk import (
    aws_iam as iam,
    aws_redshift as redshift,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_ec2 as ec2,
    core
)

class MwaaRedshiftStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, vpc, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # create s3 bucket that redshift will use. if this bucket exists
        # this cdk app will fail, so ensure this has not been created yet

        redshift_bucket = s3.Bucket(
            self,
            "mwaa-redshift import",
            bucket_name=f"{props['redshifts3location'].lower()}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        # create the files folder in the bucket - this is empty but needed in the DAG

        s3deploy.BucketDeployment(self, "File",
        sources=[s3deploy.Source.asset("./files")],
        destination_bucket=redshift_bucket,
        destination_key_prefix="files/",
        prune=False,
        retain_on_delete=False
        )

        redshift_bucket_arn = redshift_bucket.bucket_arn

        # get arn of dags bucket - not sure if this is needed so may remove
        
        dags_bucket = s3.Bucket.from_bucket_name(self, "mwaa-dag-bucket", f"{props['mwaadag'].lower()}")
        dags_bucket_arn = dags_bucket.bucket_arn

        # create redshift secret and redshift user

        # create redshift iam role/policy that we will attach to the RedShift cluster
        # that has the right level of access to a specific S3 bucket
        # you can further lockdown this policy by just specifying s3 actions.

        mwaa_redshift_policy_document = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:*"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{redshift_bucket_arn}/*",
                        f"{redshift_bucket_arn}",
                        f"{dags_bucket_arn}/*",
                        f"{dags_bucket_arn}",
                        ]
                )
            ]
        )

        mwaa_redshift_service_role = iam.Role(
            self,
            "mwaa-redshift-service-role2nd",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            inline_policies={"mwaaRedshiftPolicyDocument": mwaa_redshift_policy_document}
        )

        mwaa_redshift_service_role_arn = mwaa_redshift_service_role.role_arn

        # Setup Security Group

        default_redshift_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "MWAARedshiftSG",
            security_group_id=vpc.vpc_default_security_group
            )

        default_redshift_security_group.add_ingress_rule(
            peer=default_redshift_security_group,
            connection=ec2.Port.tcp(5439)
            )

        # Modify MWAA security group to enable Redshift access

        mwaa_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "SG",
            props['mwaa-sg']
            #mutable=False
            )
        mwaa_security_group.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(5439), "allow redshift access")
        

        # create subnet groups - one for RedShift and one for the VPE we will create
        # the VPE subnet will take in parameters we provide that are the subnet-ids
        # of the VPC where MWAA is deployed

        redshift_cluster_subnet_group = redshift.ClusterSubnetGroup(
            self,
            "RedshiftCSG",
            vpc = vpc,
            #vpc_subnets = ec2.SubnetSelection(subnets=vpc.private_subnets),
            vpc_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE),
            description="Redshift Cluster Subnet Group"
        )

        ## get all the subnet ids from the MWAA VPC

        subnet_ids = []
        mwaavpc = ec2.Vpc.from_lookup(
            self,
            "MWAA VPC",
            vpc_id=props['mwaa-vpc-id']
        )
        for subnet in mwaavpc.private_subnets:
            subnet_ids.append(subnet.subnet_id)
        for subnet in mwaavpc.public_subnets:
            subnet_ids.append(subnet.subnet_id)
        
        vpe_redshift_cluster_subnet_group = redshift.CfnClusterSubnetGroup(
            self,
            "MWAAVPERedshiftCSG",
            subnet_ids = subnet_ids,
            description="MWAA VPE Redshift Cluster Subnet Group"
        )

        redshiftclustername = f"{props['redshiftclustername'].lower()}"

        cluster = redshift.Cluster(
            self,
            "MWAARedshiftCluster",
             master_user=redshift.Login(
                master_username=props['redshiftusername']
            ),
            vpc = vpc,
            security_groups=[default_redshift_security_group],
            node_type=redshift.NodeType.RA3_4XLARGE,
            number_of_nodes=2,
            cluster_name=redshiftclustername,
            default_database_name=props['redshiftdb'],
            removal_policy=core.RemovalPolicy.DESTROY,
            roles=[mwaa_redshift_service_role],
            publicly_accessible=False,
            subnet_group=redshift_cluster_subnet_group
        )

        redshift_secret_arn = cluster.secret.secret_arn
        

        # Display some useful output

        core.CfnOutput(
            self,
            id="RedshiftSecretARN :",
            value=redshift_secret_arn,
            description="This is the Redshift Secret ARN"
        )

        core.CfnOutput(
            self,
            id="RedshiftIAMARN :",
            value=mwaa_redshift_service_role_arn,
            description="This is the Redshift IAM ARN"
        )

        core.CfnOutput(
            self,
            id="RedshiftClusterEndpoint :",
            value=cluster.cluster_endpoint.hostname,
            description="This is the Redshift Cluster Endpoint"
        )
        core.CfnOutput(
            self,
            id="MWAAVPCESG :",
            value=vpe_redshift_cluster_subnet_group.ref,
            description="This is the VPE Subnet Group to use when creating the VPC Endpoint"
        )
        core.CfnOutput(
            self,
            id="redshiftvpcendpointcli",
            value="aws redshift create-endpoint-access --cluster-identifier "+redshiftclustername+" --resource-owner "+self.account+ " --endpoint-name mwaa-redshift-endpoint --subnet-group-name "+vpe_redshift_cluster_subnet_group.ref+" --vpc-security-group-ids "+props['mwaa-sg'],
            description="Use this command to create your vpce"
        )        
