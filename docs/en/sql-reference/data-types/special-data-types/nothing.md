---
sidebar_position: 60
sidebar_label: Nothing
---

# Nothing {#nothing}

The only purpose of this data type is to represent cases where a value is not expected. So you can’t create a `Nothing` type value.

For example, literal [NULL](../../../sql-reference/syntax.md#null-literal) has type of `Nullable(Nothing)`. See more about [Nullable](../../../sql-reference/data-types/nullable.md).

The `Nothing` type can also used to denote empty arrays:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

[Original article](https://clickhouse.com/docs/en/data_types/special_data_types/nothing/) <!--hide-->
