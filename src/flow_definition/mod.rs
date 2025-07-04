use tucana::{
    aquila::{
        DataTypeUpdateRequest, FlowTypeUpdateRequest, RuntimeFunctionDefinitionUpdateRequest,
        data_type_service_client::DataTypeServiceClient,
        flow_type_service_client::FlowTypeServiceClient,
        runtime_function_definition_service_client::RuntimeFunctionDefinitionServiceClient,
    },
    shared::{DefinitionDataType as DataType, FlowType, RuntimeFunctionDefinition},
};

pub struct FlowUpdateService {
    aquila_url: String,
    data_types: Vec<DataType>,
    runtime_definitions: Vec<RuntimeFunctionDefinition>,
    flow_types: Vec<FlowType>,
}

impl FlowUpdateService {
    pub fn from_url(aquila_url: String) -> Self {
        Self {
            aquila_url,
            data_types: Vec::new(),
            runtime_definitions: Vec::new(),
            flow_types: Vec::new(),
        }
    }

    pub fn with_flow_types(mut self, flow_types: Vec<FlowType>) -> Self {
        self.flow_types = flow_types;
        self
    }

    pub fn with_data_types(mut self, data_types: Vec<DataType>) -> Self {
        self.data_types = data_types;
        self
    }

    pub fn with_runtime_definitions(
        mut self,
        runtime_definitions: Vec<RuntimeFunctionDefinition>,
    ) -> Self {
        self.runtime_definitions = runtime_definitions;
        self
    }

    pub async fn send(&self) {
        self.update_data_types().await;
        self.update_runtime_definitions().await;
        self.update_flow_types().await;
    }

    async fn update_data_types(&self) {
        if self.data_types.is_empty() {
            log::info!("No data types to update");
            return;
        }

        log::info!("Updating the current DataTypes!");
        let mut client = match DataTypeServiceClient::connect(self.aquila_url.clone()).await {
            Ok(client) => {
                log::info!("Successfully connected to the DataTypeService");
                client
            }
            Err(err) => {
                log::error!("Failed to connect to the DataTypeService: {:?}", err);
                return;
            }
        };

        let request = DataTypeUpdateRequest {
            data_types: self.data_types.clone(),
        };

        match client.update(request).await {
            Ok(response) => {
                log::info!(
                    "Was the update of the DataTypes accepted by Sagittarius? {}",
                    response.into_inner().success
                );
            }
            Err(err) => {
                log::error!("Failed to update data types: {:?}", err);
            }
        }
    }

    async fn update_runtime_definitions(&self) {
        if self.runtime_definitions.is_empty() {
            log::info!("No runtime definitions to update");
            return;
        }

        log::info!("Updating the current RuntimeDefinitions!");
        let mut client =
            match RuntimeFunctionDefinitionServiceClient::connect(self.aquila_url.clone()).await {
                Ok(client) => {
                    log::info!("Connected to RuntimeFunctionDefinitionService");
                    client
                }
                Err(err) => {
                    log::error!(
                        "Failed to connect to RuntimeFunctionDefinitionService: {:?}",
                        err
                    );
                    return;
                }
            };

        let request = RuntimeFunctionDefinitionUpdateRequest {
            runtime_functions: self.runtime_definitions.clone(),
        };

        match client.update(request).await {
            Ok(response) => {
                log::info!(
                    "Was the update of the RuntimeFunctionDefinitions accepted by Neptune? {}",
                    response.into_inner().success
                );
            }
            Err(err) => {
                log::error!("Failed to update runtime function definitions: {:?}", err);
            }
        }
    }

    async fn update_flow_types(&self) {
        if self.flow_types.is_empty() {
            log::info!("No FlowTypes to update!");
            return;
        }

        log::info!("Updating the current FlowTypes!");
        let mut client = match FlowTypeServiceClient::connect(self.aquila_url.clone()).await {
            Ok(client) => {
                log::info!("Connected to FlowTypeService!");
                client
            }
            Err(err) => {
                log::error!("Failed to connect to FlowTypeService: {:?}", err);
                return;
            }
        };

        let request = FlowTypeUpdateRequest {
            flow_types: self.flow_types.clone(),
        };

        match client.update(request).await {
            Ok(response) => {
                log::info!(
                    "Was the update of the FlowTypes accepted by Neptune? {}",
                    response.into_inner().success
                );
            }
            Err(err) => {
                log::error!("Failed to update flow types: {:?}", err);
            }
        }
    }
}