use bollard::container::{Config, CreateContainerOptions, StartContainerOptions};
use bollard::secret::HostConfig;
use bollard::Docker;

pub fn get_docker_client() -> Result<Docker, bollard::errors::Error> {
    Docker::connect_with_local_defaults()
}

pub async fn start_pg() -> Result<(String, String), bollard::errors::Error> {
    let docker = get_docker_client()?;

    let image = "postgres:15.1-alpine3.16";

    let config = Config {
        image: Some(image),
        env: Some(vec![
            "POSTGRES_USER=postgres",
            "POSTGRES_DB=test",
            "POSTGRES_PASSWORD=password",
        ]),
        host_config: Some(HostConfig {
            auto_remove: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };

    let options: Option<CreateContainerOptions<String>> = None;

    let container = docker.create_container(options, config).await?;

    docker
        .start_container(&container.id, None::<StartContainerOptions<String>>)
        .await
        .unwrap();

    let inspect = docker.inspect_container(&container.id, None).await?;
    let ip = inspect.network_settings.unwrap().ip_address.unwrap();

    Ok((container.id, ip))
}

pub async fn end_pg(container_id: &str) -> Result<(), bollard::errors::Error> {
    let docker = get_docker_client()?;
    docker.stop_container(container_id, None).await?;
    Ok(())
}
