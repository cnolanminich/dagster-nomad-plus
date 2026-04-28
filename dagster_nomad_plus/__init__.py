from dagster_shared.libraries import DagsterLibraryRegistry

from dagster_nomad_plus.client import (
    NomadAuth as NomadAuth,
    NomadClient as NomadClient,
)
from dagster_nomad_plus.container_context import NomadContainerContext as NomadContainerContext
from dagster_nomad_plus.job_spec import (
    NomadJobSpecBuilder as NomadJobSpecBuilder,
    build_run_job_spec as build_run_job_spec,
)
from dagster_nomad_plus.run_launcher import NomadRunLauncher as NomadRunLauncher
from dagster_nomad_plus.version import __version__ as __version__

DagsterLibraryRegistry.register("dagster-nomad-plus", __version__)
