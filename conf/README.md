# Config File Template

This is show the standard template of config file, which used in framework

## Catalog

- #### SQL Process catalog
    - `/catalog/<prefix-table-name-with-detail>.yaml`

```yaml
<table-name>:
    version: "yyyy-mm-dd"
    create:
         features:
              <column-name-01>: "<datatype> <nullable-property>"
              <column-name-02>: "<datatype> <nullable-property>"
              ...
         primary_key: ["<primary-key01>", "<primary-key02>", ...]
    initial:
        parameter: ["<param01>", "<param02>", ...]
        statement: "
            insert into {database-name-param}.{schema-name-param}.<table-name>
            ...
            "
    update:
        <process-name01>:
            priority: 1
            parameter: ["<param01>", "<param02>", ...]
            statement: "
                insert into {database-name-param}.{schema-name-param}.<table-name>
                ...
                "
        <process-name02>:
            priority: 2
            parameter: ["<param01>", "<param02>", ...]
            statement: "
                insert into {database-name-param}.{schema-name-param}.<table-name>
                ...
                "
    
```

- #### Function Process Catalog

    - `/catalog/<prefix-table-name-with-detail>.yaml`

```yaml
<table-name>:
    version: "yyyy-mm-dd"
    create:
         features:
              <column-name-01>: "<datatype> <nullable-property>"
              <column-name-02>: "<datatype> <nullable-property>"
              ...
         primary_key: ["<primary-key01>", "<primary-key02>", ...]
    initial:
        parameter: ["<param01>", "<param02>", ...]
        statement: "
            insert into {database-name-param}.{schema-name-param}.<table-name>
            ...
            "
    load:
        parameter: ["<param01>", "<param02>", ...]
        statement: "
            ...
            select  ...
            from    {database-name-param}.{schema-name-param}.<table-name>
            ... 
            "
    save:
        parameter: ["function_value", "<param01>", "<param02>", ...]
        statement: "
            insert into {database-name-param}.{schema-name-param}.<table-name>
            (
                ...
            )
            values {function_value}
                ...
            )
            "
    
```
*Note:* In the save attribute must add parameter `function_value` for keep output of function to insert statement

## Function

- #### Function
    - `/function/<function-mapping-name>.yaml`

```yaml
<function-name>:
    version: "yyyy-mm-dd"
    parameter: ["<param01>", "<param02>", ...]
    create: "
        ...
        create or replace function {database-name-param}.{schema-name-param}.<function-name>(
            <input> <type>, out <output> <type>
        ) language plpgsql as
        $func$
            ...
        $func$
        ...
        "
    
```
