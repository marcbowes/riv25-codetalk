import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as path from 'path';

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Get cluster endpoint from context
    const clusterEndpoint = this.node.tryGetContext('clusterEndpoint');

    const lambdaFunction = new nodejs.NodejsFunction(this, 'Ch07Function', {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, '../../lambda/src/index.ts'),
      handler: 'handler',
      functionName: 'ch07',
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      environment: {
        CLUSTER_ENDPOINT: clusterEndpoint || '',
        REGION: this.region,
      },
      bundling: {
        forceDockerBundling: false,
      },
    });

    // Add DSQL permissions
    lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'dsql:DbConnect',
        'dsql:DbConnectAdmin'
      ],
      resources: ['*']
    }));

    new cdk.CfnOutput(this, 'FunctionName', {
      value: lambdaFunction.functionName,
      description: 'Lambda Function Name'
    });

    new cdk.CfnOutput(this, 'FunctionArn', {
      value: lambdaFunction.functionArn,
      description: 'Lambda Function ARN'
    });

    if (clusterEndpoint) {
      new cdk.CfnOutput(this, 'ClusterEndpoint', {
        value: clusterEndpoint,
        description: 'DSQL Cluster Endpoint'
      });
    }
  }
}
