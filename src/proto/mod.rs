use tonic::{Request, Response, Status};

pub(crate) mod hello_world {
    tonic::include_proto!("helloworld");
}

#[derive(Debug, Default)]
pub(crate) struct GreeterImpl {}

#[tonic::async_trait]
impl hello_world::greeter_server::Greeter for GreeterImpl {
    async fn say_hello(
        &self,
        request: Request<hello_world::HelloRequest>,
    ) -> Result<Response<hello_world::HelloReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = hello_world::HelloReply {
            message: format!("Hello {}!", request.into_inner().name),
        };

        Ok(Response::new(reply))
    }
}
