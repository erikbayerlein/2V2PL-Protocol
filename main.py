from protocol.inputReader.input_reader import InputReader


def main():
    entrada_ai = "r4(v)r3(y)r1(y)r1(x)w2(u)r2(x)w1(y)r2(y)c1w4(u)r3(x)c4w3(u)w3(z)c3"
    # entrada_ai = "w2(u)w3(u)w4(u)w4(x)w2(x)"

    operations = InputReader.read_input(entrada_ai)
    for op in operations:
        print(f"{op.get_id()}, {op.get_type()}, {op.object}")
    # scheduler = Scheduler()
    # print(scheduler.schedule(operations))


if __name__ == "__main__":
    main()
