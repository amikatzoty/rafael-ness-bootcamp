import docker
import time


class DockerManager:
    def __init__(self):
        self.cli = docker.from_env()
        self.containers = self.cli.containers.list()
        self.containers_dict = {c.image.tags[0]: c for c in self.containers}

    def start(self, container_name):
        self.containers_dict[container_name].start()

    def stop(self, container_name):
        self.containers_dict[container_name].stop()

    def restart(self, container_name):
        self.containers_dict[container_name].restart()

    def pause(self, container_name):
        self.containers_dict[container_name].pause()

    def unpause(self, container_name):
        self.containers_dict[container_name].unpause()


if __name__ == '__main__':
    dm = DockerManager()
    dm.stop('eshop/ordering.api:linux-latest')
    time.sleep(1)
    dm.start('eshop/ordering.api:linux-latest')

    dm.pause('eshop/ordering.api:linux-latest')
    time.sleep(1)
    dm.unpause('eshop/ordering.api:linux-latest')

    dm.restart('eshop/ordering.api:linux-latest')
