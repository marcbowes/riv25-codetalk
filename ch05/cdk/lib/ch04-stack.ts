import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as path from 'path';

export class Ch04Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const clusterEndpoint = this.node.tryGetContext('clusterEndpoint') || process.env.CLUSTER_ENDPOINT;
    if (!clusterEndpoint) {
      throw new Error('clusterEndpoint must be provided via context or CLUSTER_ENDPOINT env var');
    }

    const lambdaFunction = new nodejs.NodejsFunction(this, 'Ch04Function', {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, '../../lambda/src/index.ts'),
      handler: 'handler',
      functionName: 'ch04',
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
      environment: {
        CLUSTER_ENDPOINT: clusterEndpoint,
        REGION: this.region
      },
      bundling: {
        externalModules: ['aws-sdk'],
        nodeModules: ['@aws-sdk/dsql-signer', 'postgres']
      }
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
  }
}
