export function toNumber(n: number | Long): number {
    if (typeof n === "number") {
        return n;
    } else {
        return n.toNumber();
    }
}
