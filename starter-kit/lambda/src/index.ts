import { Handler } from "aws-lambda";

interface Request {
  name: string;
}

interface Response {
  greeting: string;
}

export const handler: Handler<Request, Response> = async (event) => {
  return {
    greeting: `hello ${event.name}`,
  };
};
