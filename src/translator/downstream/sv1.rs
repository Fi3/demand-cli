use super::downstream::Downstream;
use sv1_api::{
    client_to_server,
    error::Error,
    json_rpc, server_to_client,
    utils::{Extranonce, HexU32Be},
    IsServer,
};

impl IsServer for Downstream {
    type Error = Error;

    fn handle_configure(
        &mut self,
        _client_id: Option<usize>,
        request: &client_to_server::Configure,
    ) -> Result<(Option<server_to_client::VersionRollingParams>, Option<bool>), Error> {
        Ok(Downstream::handle_configure(self, request))
    }

    fn handle_subscribe(
        &self,
        _client_id: Option<usize>,
        request: &client_to_server::Subscribe,
    ) -> Result<Vec<(String, String)>, Error> {
        Ok(Downstream::handle_subscribe(self, request))
    }

    fn handle_authorize(
        &self,
        _client_id: Option<usize>,
        request: &client_to_server::Authorize,
    ) -> Result<bool, Error> {
        Ok(Downstream::handle_authorize(self, request))
    }

    fn handle_submit(
        &self,
        _client_id: Option<usize>,
        _request: &client_to_server::Submit,
    ) -> Result<client_to_server::SubmitOutcome, Error> {
        Ok(client_to_server::SubmitOutcome::Rejected(
            client_to_server::SubmitError::Other,
        ))
    }

    fn handle_extranonce_subscribe(&mut self, _client_id: Option<usize>) -> Result<(), Error> {
        Ok(())
    }

    fn is_authorized(&self, _client_id: Option<usize>, name: &str) -> Result<bool, Error> {
        Ok(Downstream::is_authorized(self, name))
    }

    fn authorize(&mut self, _client_id: Option<usize>, name: &str) -> Result<(), Error> {
        Downstream::authorize(self, name);
        Ok(())
    }

    fn set_extranonce1(
        &mut self,
        _client_id: Option<usize>,
        extranonce: Option<Extranonce>,
    ) -> Result<Extranonce, Error> {
        Ok(Downstream::set_extranonce1(self, extranonce))
    }

    fn extranonce1(&self, _client_id: Option<usize>) -> Result<Extranonce, Error> {
        Ok(Downstream::extranonce1(self))
    }

    fn set_extranonce2_size(
        &mut self,
        _client_id: Option<usize>,
        size: Option<usize>,
    ) -> Result<usize, Error> {
        Ok(Downstream::set_extranonce2_size(self, size))
    }

    fn extranonce2_size(&self, _client_id: Option<usize>) -> Result<usize, Error> {
        Ok(Downstream::extranonce2_size(self))
    }

    fn version_rolling_mask(&self, _client_id: Option<usize>) -> Result<Option<HexU32Be>, Error> {
        Ok(Downstream::version_rolling_mask(self))
    }

    fn set_version_rolling_mask(
        &mut self,
        _client_id: Option<usize>,
        mask: Option<HexU32Be>,
    ) -> Result<(), Error> {
        Downstream::set_version_rolling_mask(self, mask);
        Ok(())
    }

    fn set_version_rolling_min_bit(
        &mut self,
        _client_id: Option<usize>,
        mask: Option<HexU32Be>,
    ) -> Result<(), Error> {
        Downstream::set_version_rolling_min_bit(self, mask);
        Ok(())
    }

    fn notify(&mut self, _client_id: Option<usize>) -> Result<json_rpc::Message, Error> {
        Err(Error::UnexpectedMessage("mining.notify".to_string()))
    }
}
