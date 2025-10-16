import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import { Construct } from 'constructs';
import * as path from 'path';

export class Ch01Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const lambdaFunction = new nodejs.NodejsFunction(this, 'Ch01Function', {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, '../../lambda/src/index.ts'),
      handler: 'handler',
      functionName: 'ch01',
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
      bundling: {
        forceDockerBundling: false,
      },
    });

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
