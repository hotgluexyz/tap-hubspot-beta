import json, pathlib, uuid, subprocess, os
from logging import Logger
from typing import Union, Optional

class RealTime:
    def __init__(
            self,
            command: str,
            config: dict,
            logger: Logger,
        ):
        self.command = command
        self.config = config
        self.logger = logger
        self.id = str(uuid.uuid4())
        self.config_file_path = f"/tmp/{self.id}.config.json"
        self.catalog_file_path = f"/tmp/{self.id}.catalog.json"
        os.makedirs("/tmp", exist_ok=True)

    def _create_config_file(self):
        with open(self.config_file_path, "w") as f:
            f.write(json.dumps(self.config))

    def _delete_catalog_file(self):
        pathlib.Path(self.catalog_file_path).unlink(missing_ok=True)

    def prepare(self):
        self._create_config_file()

    def discover(self):
        command = f"{self.command} --config {self.config_file_path} --discover > {self.catalog_file_path}"
        self.logger.info(f"Running command: {command}")
        proc = subprocess.run(
            command,
            shell=True,
            text=True,
            capture_output=True
        )

        logs = proc.stdout.strip() or proc.stderr.strip()

        self.logger.info(logs)

        return {
            "tracebackInLogs": "Traceback" in logs,
            "logs": logs,
        }

    def get_catalog(self) -> Union[dict, str]:
        with open(self.catalog_file_path, "r") as f:
            lines = f.readlines()
            try:
                return json.loads(lines[-1].strip())
            except:
                return "".join(lines)

    def clean_up(self):
        self._delete_catalog_file()


def real_time_handler(
    config: dict,
    logger: Logger,
    discover: bool = False,
    cli_cmd: Optional[str] = None,
):
    cli_cmd = cli_cmd or os.environ.get("CLI_CMD")

    if not cli_cmd:
        logger.info(f"Parameter cli_cmd or CLI_CMD env var are not set. This target does not support real time")
        raise Exception("This target does not support real time")

    logger.info(f"Entering \"real_time_handler\": cli_cmd={cli_cmd}, config={config}, discover={discover}")

    real_time = RealTime(
        cli_cmd,
        config,
        logger,
    )

    response = {"discoverCatalog": None, "metrics": dict()}

    logger.info(f"Preparing files...")

    real_time.prepare()

    if discover:
        logger.info(f"Running discover...")
        discover_metrics = real_time.discover()
        logger.info(f"Getting catalog...")
        response["discoverCatalog"] = real_time.get_catalog()
        response["discoverMetrics"] = discover_metrics
        response["metrics"]["tracebackInLogs"] = discover_metrics["tracebackInLogs"]
        response["metrics"]["logs"] = discover_metrics["logs"]

    logger.info(f"Cleaning up...")

    real_time.clean_up()

    logger.info(f"Done")

    return response
