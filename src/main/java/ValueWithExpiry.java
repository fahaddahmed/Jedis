/**
 * Class to represent a value with an optional expiry time.
 */

 class ValueWithExpiry {
    String value;
    long expiryTime;

    public ValueWithExpiry(String value, long expiryTime) {
        this.value = value;
        this.expiryTime = expiryTime;
    }
}