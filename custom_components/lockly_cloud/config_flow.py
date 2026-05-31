"""Config flow for the Lockly Cloud integration."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from pylockly import LocklyAPI
from pylockly.exceptions import LocklyAuthError, LocklyError

from .const import CONF_EMAIL, CONF_PASSWORD, DOMAIN

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): str,
        vol.Required(CONF_PASSWORD): str,
    }
)


class LocklyConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Lockly."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step: email + password."""
        errors: dict[str, str] = {}

        if user_input is not None:
            email = user_input[CONF_EMAIL]
            password = user_input[CONF_PASSWORD]

            await self.async_set_unique_id(email.lower())
            self._abort_if_unique_id_configured()

            api = LocklyAPI()
            try:
                locks = await api.login(email, password)
            except LocklyAuthError:
                errors["base"] = "invalid_auth"
            except LocklyError:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error during Lockly login")
                errors["base"] = "unknown"
            else:
                await api.close()
                return self.async_create_entry(
                    title=f"Lockly ({email})",
                    data={
                        CONF_EMAIL: email,
                        CONF_PASSWORD: password,
                    },
                )
            finally:
                await api.close()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )
