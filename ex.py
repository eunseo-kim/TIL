class SRTN(Scheduler):
    def run(self):
        cur_time = 0  # 시간 증가
        at_idx = 0
        finished_processes_count = 0  # 종료된 프로세스 = 총 프로세스

        sorted_processes = sorted(self.processes, key=lambda x: x.at)

        while finished_processes_count < self.process_count:
            for process_idx in range(at_idx, self.process_count):
                process = sorted_processes[process_idx]

                if process.at == cur_time:
                    self.ready_queue.append(process)

                elif process.at > cur_time:
                    at_idx = process_idx
                    break

            # history 기록하기
            self.record_history(self.ready_queue[:], self.cpus, self.processes)

            self.ready_queue = sorted(self.ready_queue, key=lambda x: x.remain_bt)

            for cpu in self.cpus:
                if cpu.is_finished():
                    cpu.process.calculate_finished_process(cur_time)
                    finished_processes_count += 1
                    cpu.set_idle()

                if cpu.is_idle():  # 쉬고 있는 CPU에는 바로 프로세스를 넣어줌
                    if self.ready_queue:
                        next_process = self.ready_queue.pop(0)
                        cpu.process = next_process

            if self.ready_queue:  # ready_queue가 있으면 그 후 비교
                # cpu 리스트 생성
                cpu_list = []
                for cpu in self.cpus:
                    cpu_list.append(cpu)

                cpu_list.sort(key=lambda x: x.process.remain_bt)  # 오름차순으로 정렬

                max_idx = min(self.cpu_count, len(self.ready_queue))  # 비교 가능 최대 횟수
                for process_check_idx in range(max_idx):
                    back_check_idx = -(process_check_idx + 1)
                    if cpu_list[back_check_idx].process.remain_bt > self.ready_queue[process_check_idx].remain_bt:
                        swap_process = cpu_list[back_check_idx].process
                        cpu_list[back_check_idx].set_process(self.ready_queue.pop(0))
                        self.ready_queue.append(swap_process)
                    else:
                        break

            cur_time += 1
            super().work()
