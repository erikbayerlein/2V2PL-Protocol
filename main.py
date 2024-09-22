from scheduler import Scheduler
from input_reader import InputReader
from objects import Objects


def main():
    schema = Objects.create_schema(2, 2, 2, 2)

    input_str = "r1(RW16)r2(RW2)u2(RW3)w2(RW3)r2(RW3)c2r1(RW3)c1"
    # input_str = "w1(RW16)r2(RW16)r1(RW3)w2(RW3)c1c2"
    input_parsed = InputReader.parse_operations_string(input_str, schema)

    scheduler = Scheduler()
    scheduler.schedule(input_parsed)


if __name__ == "__main__":
    main()
