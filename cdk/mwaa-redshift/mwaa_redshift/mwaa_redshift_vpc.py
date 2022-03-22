from aws_cdk import core
import aws_cdk.aws_ec2 as ec2



class MwaaRedshiftVPC(core.Stack):

    def __init__(self, scope: core.Construct, id: str, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
   
        # Create VPC network

        self.vpc = ec2.Vpc(
            self,
            id="MWAA-RedShiftCluster-VPC",
            cidr="10.192.0.0/16",
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public", cidr_mask=20,
                    reserved=False, subnet_type=ec2.SubnetType.PUBLIC),
                ec2.SubnetConfiguration(
                    name="private", cidr_mask=20,
                    reserved=False, subnet_type=ec2.SubnetType.PRIVATE)
            ],
            max_azs=2,
            enable_dns_hostnames=True,
            enable_dns_support=True
        )

        core.CfnOutput(
            self,
            id="VPCId",
            value=self.vpc.vpc_id,
            description="VPC ID",
            export_name=f"{self.region}:{self.account}:{self.stack_name}:vpc-id"
        )
