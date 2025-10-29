from locustfile import HttpUser, task, between

class APIUser(HttpUser):
    wait_time = between(1, 3)

    @task
    def health(self):
        self.client.get("/health")

    @task
    def api_health(self):
        self.client.get("/api/v1/health")

    @task
    def get_energy_readings(self):
        self.client.get("/api/v1/energy-readings?limit=5")

    @task
    def get_energy_summary(self):
        self.client.get("/api/v1/energy-summary")

    @task
    def get_stats(self):
        self.client.get("/api/v1/stats")

    @task
    def get_appliance_types(self):
        self.client.get("/api/v1/appliance-types")

    @task
    def get_homes(self):
        self.client.get("/api/v1/homes")

    @task
    def get_consumption_by_season(self):
        self.client.get("/api/v1/consumption-by-season")

    @task
    def get_consumption_by_appliance(self):
        self.client.get("/api/v1/consumption-by-appliance")

    @task
    def get_daily_consumption(self):
        self.client.get("/api/v1/daily-consumption")

    @task
    def get_high_consumption(self):
        self.client.get("/api/v1/high-consumption")

    @task
    def get_temperature_analysis(self):
        self.client.get("/api/v1/temperature-analysis")
