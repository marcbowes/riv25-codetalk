#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { Dat401Stack } from "../lib/dat401-stack";

const app = new cdk.App();
new Dat401Stack(app, "ReinventDat401Stack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
