import time
from random import randint
import uuid
import etcd3

class Programm:

    def __init__(self, cluster_name):
        self.my_token = uuid.uuid4().__str__()
        self.master_token = None
        self.fs_profile_is_sopped = False
        self.min_waiting_time = 0
        self.max_waiting_time = 1
        self.token_lease = 20
        self.client = None
        self.key = cluster_name + "/master_key"


    def get_etcd_client(self):
        cli = None
        try:
            cli = etcd3.client(host='34.69.186.246', port=2379)
            print(cli.status())
        except:
            print("error connection to etcd")
        finally:
            if cli:
                cli.close()
                cli = None
        return cli

    def get_master_token(self):
        token = None
        cli = self.get_etcd_client()
        if cli:
            token = cli.get(self.key)[0]
            cli.close()
        return token

    def try_set_new_master_token(self):
        cli = self.get_etcd_client()
        if cli:
            cli.put(self.key, self.my_token, lease=cli.lease(self.token_lease))
            cli.close()
            return True
        else:
            return False

    def update_master_token(self):
        cli = self.get_etcd_client()
        if cli:
            cli.put(self.key, self.my_token, lease=cli.lease(self.token_lease))
            cli.close()


    def start_fs_profiles(self):
        print("FS profiles started")

    def stop_fs_profiles(self):
        if not self.fs_profile_is_sopped:
            self.fs_profile_is_sopped = True
            print("stop profiles")
        else:
            print("fs profiles allready stopped")

    def wait(self):
        sleep_time = randint(self.min_waiting_time, self.max_waiting_time)
        time.sleep(sleep_time)

    def im_master(self):
        return self.my_token == self.get_master_token()


    def watch(self):
        while True:
            self.wait()
            if self.im_master():
                self.update_master_token()
            else:
                if self.try_set_new_master_token():
                    self.start_fs_profiles()
                else:
                    self.stop_fs_profiles()



if __name__ == "__main__":
    p = Programm(cluster_name="super")
    p.watch();
