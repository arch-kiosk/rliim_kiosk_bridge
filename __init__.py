import logging

from sync_plugins.rliim_kiosk_bridge.rliim_import.rliim2kioskimport import import_rliim_cm_for_new_loci
from sync_config import SyncConfig
from synchronization import Synchronization
from synchronizationplugin import SynchronizationPlugin

_plugin_ = None


class PluginRLIIMKioskBridgeHook(SynchronizationPlugin):
    _plugin_version = 1.4

    def all_plugins_ready(self):
        app: Synchronization = self.app
        app.events.subscribe("synchronization", "after_synchronization", self.trigger_rliim_to_kiosk_import)
        logging.debug(f"PluginRLIIMKioskBridgeHook subscribed to synchronization.after_synchronization")

    def trigger_rliim_to_kiosk_import(self):
        logging.debug(f"{self.__class__.__name__}.trigger_rliim_to_kiosk_import: called.")
        config = SyncConfig.get_config()
        if config.get_project_id() not in ["rliim"]:
            logging.error(f"PluginRLIIMKioskBridgeHook.trigger_rliim_to_kiosk_import: "
                            f"This plugin cannot run for project with id {config.get_project_id()}")
            return None
        return import_rliim_cm_for_new_loci(commit=True)

# -----------------------------------------------------------
# Plugin - Code
# -----------------------------------------------------------
def instantiate_plugin_object(plugin_candidate, package, init_plugin_configuration={}):
    config = SyncConfig.get_config()
    if config.get_project_id() not in  ["rliim"]:
        logging.info(f"PluginRLIIMKioskBridgeHook.instantiate_plugin_object: "
                      f"This plugin cannot run for project with id {config.get_project_id()}")
        return None

    logging.debug(f"PluginRLIIMKioskBridgeHook installed ")
    return PluginRLIIMKioskBridgeHook(plugin_candidate, package)
