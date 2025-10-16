import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as dsql from 'aws-cdk-lib/aws-dsql';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as path from 'path';

export class Dat401Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create DSQL cluster
    const cluster = new dsql.CfnCluster(this, 'DsqlCluster', {
      deletionProtectionEnabled: false,
      tags: [{
        key: 'Name',
        value: 'DAT401'
      }]
    });

    // Construct cluster endpoint
    const clusterEndpoint = `${cluster.attrIdentifier}.dsql.${this.region}.on.aws`;

    const lambdaFunction = new nodejs.NodejsFunction(this, 'ReinventDat401Function', {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, '../../lambda/src/index.ts'),
      handler: 'handler',
      functionName: 'reinvent-dat401',
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
      environment: {
        CLUSTER_ENDPOINT: clusterEndpoint
      }
    });

    // Add DSQL DbConnect permission for myapp role
    lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['dsql:DbConnect'],
      resources: [cluster.attrResourceArn]
    }));

    // Output the cluster endpoint for easy access
    new cdk.CfnOutput(this, 'ClusterEndpoint', {
      value: clusterEndpoint,
      description: 'DSQL Cluster Endpoint'
    });

    // Output the Lambda execution role ARN
    new cdk.CfnOutput(this, 'LambdaRoleArn', {
      value: lambdaFunction.role!.roleArn,
      description: 'Lambda Execution Role ARN'
    });
  }
}
