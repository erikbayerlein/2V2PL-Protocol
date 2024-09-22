

def main():
    input = "r2(v)r1(x)w2(x)r3(v)r1(y)w3(y)r2(z)w3(z)c3c1c2"
    operations = InputReader.read_input(input)
    # for op in operations:
    #   print(f"{op.get_id()}, {op.get_transaction_id()}, {op.get_type()}, {op.object}")
    scheduler = Scheduler()
    op = scheduler.schedule(operations)
    # for s in op:
    #    print(s)


if __name__ == "__main__":
    main()
