use ruma::api::appservice::Registration;

use crate::{database::KeyValueDatabase, service, utils, Error, Result};

impl service::appservice::Data for KeyValueDatabase {
    /// Registers an appservice and returns the ID to the caller
    fn register_appservice(&self, yaml: Registration) -> Result<String> {
        let id = yaml.id.as_str();
        self.id_appserviceregistrations.insert(
            id.as_bytes(),
            serde_yaml::to_string(&yaml).unwrap().as_bytes(),
        )?;
        self.cached_registrations
            .write()
            .unwrap()
            .insert(id.to_owned(), yaml.clone());

        Ok(id.to_owned())
    }

    /// Remove an appservice registration
    ///
    /// # Arguments
    ///
    /// * `service_name` - the name you send to register the service previously
    fn unregister_appservice(&self, service_name: &str) -> Result<()> {
        self.id_appserviceregistrations
            .remove(service_name.as_bytes())?;
        self.cached_registrations
            .write()
            .unwrap()
            .remove(service_name);
        Ok(())
    }

    fn get_registration(&self, id: &str) -> Result<Option<Registration>> {
        self.cached_registrations
            .read()
            .unwrap()
            .get(id)
            .map_or_else(
                || {
                    self.id_appserviceregistrations
                        .get(id.as_bytes())?
                        .map(|bytes| {
                            serde_yaml::from_slice(&bytes).map_err(|_| {
                                Error::bad_database(
                                    "Invalid registration bytes in id_appserviceregistrations.",
                                )
                            })
                        })
                        .transpose()
                },
                |r| Ok(Some(r.clone())),
            )
    }

    fn iter_ids<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<String>> + 'a>> {
        Ok(Box::new(self.id_appserviceregistrations.iter().map(
            |(id, _)| {
                utils::string_from_bytes(&id).map_err(|_| {
                    Error::bad_database("Invalid id bytes in id_appserviceregistrations.")
                })
            },
        )))
    }

    fn all(&self) -> Result<Vec<(String, Registration)>> {
        self.iter_ids()?
            .filter_map(std::result::Result::ok)
            .map(move |id| {
                Ok((
                    id.clone(),
                    self.get_registration(&id)?
                        .expect("iter_ids only returns appservices that exist"),
                ))
            })
            .collect()
    }
}
