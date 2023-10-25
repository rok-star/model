import * as libpg from 'pg'
import * as libschema from 'schema'

export type FieldReferenceOnAction = 'no action' | 'restrict' | 'cascade' | 'set default';

export type FieldReference = {
    readonly table: string;
    readonly field: string;
    readonly onDelete?: FieldReferenceOnAction;
    readonly onUpdate?: FieldReferenceOnAction;
}

export type FieldIndexType = 'btree';

export type FieldType = 'serial' | 'integer' | 'double' | 'string';

export type Field = {
    readonly name: string;
	readonly type: FieldType;
	readonly nullable?: boolean;
    readonly unique?: boolean;
    readonly defaultValue?: string;
    readonly primaryKey?: boolean;
    readonly references?: FieldReference;
    readonly oneOf?: readonly string[];
    readonly index?: FieldIndexType;
}

export type Table = {
	readonly name: string;
	readonly fields: readonly Field[];
}

export enum SyncIssueType {
    schemaNotFound = 'schemaNotFound',
    tableNotFound = 'tableNotFound',
    fieldNotFound = 'fieldNotFound',
    fieldTypeMissmatch = 'fieldTypeMissmatch',
    fieldNullableMissmatch = 'fieldNullableMissmatch',
    fieldDefaultValueMissmatch = 'fieldDefaultValueMissmatch',
    primaryKeyNotFound = 'primaryKeyNotFound',
    primaryKeyDangling = 'primaryKeyDangling',
    foreignKeyNotFound = 'foreignKeyNotFound',
    foreignKeyDangling = 'foreignKeyDangling',
    foreignKeyMissmatch = 'foreignKeyMissmatch',
    uniqueKeyNotFound = 'uniqueKeyNotFound',
    uniqueKeyDangling = 'uniqueKeyDangling',
    checkKeyNotFound = 'checkKeyNotFound',
    checkKeyMissmatch = 'checkKeyMissmatch',
    checkKeyDangling = 'checkKeyDangling',
    btreeIndexNotFound = 'btreeIndexNotFound',
    btreeIndexDangling = 'btreeIndexDangling'
}

export type SyncIssue = {
    type: SyncIssueType;
    schema?: string;
    table?: Table;
    field?: Field;
    resolvable: boolean;
    description?: string;
    fulfilled?: boolean;
    actions: string[];
    error?: Error;
}

export type SyncOptions = {
    fullfill?: boolean;
}

export type SyncResult = {
    issues: SyncIssue[];
    actions: string[];
    warnings: string[];
    resolvable: boolean;
    fulfilled: boolean;
}

export const sync = async (client: (libpg.Client | libpg.PoolClient | libpg.Pool), schema: string, tables: Table[], options?: SyncOptions): Promise<SyncResult> => {

    const SQL_SCHEMA_LIST =
`select "nspname"
from "pg_catalog"."pg_namespace"`;

    const SQL_SCHEMA_FIELDS =
`select ns.nspname,
        tb.relname,
        col.attname,
        typ.typname,
        col.attnotnull,
        col.atthasdef,
        col.attisdropped,
        pg_get_expr(coldef.adbin, coldef.adrelid) as adbin_sql
from pg_catalog.pg_namespace ns
join pg_catalog.pg_class tb on tb.relnamespace = ns.oid
join pg_catalog.pg_attribute col on col.attrelid = tb.oid
join pg_catalog.pg_type typ on typ.oid = col.atttypid
left join pg_catalog.pg_attrdef coldef on coldef.adrelid = tb.oid and coldef.adnum = col.attnum
where tb.relkind = 'r'
and col.attnum > 0
and ns.nspname = $1
order by tb.relname, col.attname`;

    const SQL_SCHEMA_KEYS =
`select cs.contype,
        cs.conname,
        tb.relname,
        array(select col.attname
                from pg_catalog.pg_attribute col
            where col.attrelid = cs.conrelid
                and col.attnum = any(cs.conkey)) as conkeys,
        tb2.relname as relfname,
        array(select col.attname
                from pg_catalog.pg_attribute col
            where col.attrelid = cs.confrelid
                and col.attnum = any(cs.confkey)) as confkeys,
        cs.confupdtype,
        cs.confdeltype,
        pg_get_constraintdef(cs.oid) as condef
from pg_catalog.pg_namespace ns
join pg_catalog.pg_constraint cs on cs.connamespace = ns.oid
join pg_catalog.pg_class tb on tb.oid = cs.conrelid
left join pg_catalog.pg_class tb2 on tb2.oid = cs.confrelid
where cs.contype in ('p', 'f', 'u', 'c')
and ns.nspname = $1`;

    const SQL_SCHEMA_IDXS =
`select cl.relname as idxname,
        tb.relname,
        array(select col.attname
                from pg_catalog.pg_attribute col
            where col.attrelid = tb.oid
                and col.attnum = any(idx.indkey)) as attrs
from pg_catalog.pg_index idx
join pg_catalog.pg_class cl on cl.oid = idx.indexrelid
join pg_catalog.pg_class tb on tb.oid = idx.indrelid
join pg_catalog.pg_namespace ns on ns.oid = tb.relnamespace
join pg_catalog.pg_am am on am.oid = cl.relam
where am.amname = 'btree' and ns.nspname = $1`;

    const FIELD_TYPE_MAP: { class_: string, jstype: FieldType, dbtype: string, aliases: string[], convertsTo: FieldType[] }[] = [
        { class_: 'integer', jstype: 'serial', dbtype: 'bigserial', aliases: ['bigserial', 'serial8'], convertsTo: ['serial'] },
        { class_: 'integer', jstype: 'integer', dbtype: 'bigint', aliases: ['bigint', 'int8'], convertsTo: ['integer', 'double', 'string'] },
        { class_: 'double', jstype: 'double', dbtype: 'float8', aliases: ['double precision', 'float8'], convertsTo: ['double', 'string'] },
        { class_: 'text', jstype: 'string', dbtype: 'varchar', aliases: ['character varying', 'varchar', 'character', 'char'], convertsTo: ['string'] },
    ];

    const toJSType = (type: string): FieldType | undefined => {
        return FIELD_TYPE_MAP.find(e => e.aliases.some(i => i.toLowerCase() === type.toLowerCase()))?.jstype;
    }

    const toDBType = (type: FieldType): string => {
        const dbtype = FIELD_TYPE_MAP.find(e => e.jstype === type)?.dbtype;
        if (dbtype) {
            return dbtype;
        } else {
            throw new Error(`failed to convert field type "${type}" to db type`);
        }
    }

    const valueExpr = (value: string, type: FieldType): string => {
        if (type === 'string') {
            return `'${value}'`;
        } else {
            return value;
        }
    }

    const query = async <T extends libpg.QueryResultRow = any>(sql: string, args?: any[]) => {
        try {
            return await client.query<T>(sql, args);
        } catch (e) {
            throw new Error(`${e.message}: ${sql}`);
        }
    }

    type SchemaFieldsEntry = {
        nspname: string;
        relname: string;
        attname: string;
        typname: string;
        attnotnull: boolean;
        atthasdef: boolean;
        attisdropped: boolean;
        adbin_sql: string;
    }

    type SchemaKeysEntry = {
        contype: string;
        conname: string;
        relname: string;
        conkeys: string;
        relfname: string;
        confkeys: string;
        confupdtype: string;
        confdeltype: string;
        condef: string;
    }

    type SchemaBTreeEntry = {
        idxname: string;
        relname: string;
        attrs: string;
    }

    type Resolvable = {
        resolvable: boolean;
        description?: string;
    }

    const result: SyncResult = {
        issues: [],
        actions: [],
        warnings: [],
        resolvable: false,
        fulfilled: false
    };

    const keysState = (await query<SchemaKeysEntry>(SQL_SCHEMA_KEYS, [schema])).rows;
    const fieldState = (await query<SchemaFieldsEntry>(SQL_SCHEMA_FIELDS, [schema])).rows;
    const btreeState = (await query<SchemaBTreeEntry>(SQL_SCHEMA_IDXS, [schema])).rows;
    const locateField = (tableName: string, fieldName?: string) => fieldState.find(e => e.relname === tableName && (e.attname === fieldName || libschema.isNotAssigned(fieldName)));
    const locateBTree = (tableName: string, fieldName: string) => btreeState.find(e => e.relname === tableName && e.attrs.includes(`{${fieldName}}`));
    const locateCKey = (tableName: string, fieldName: string) => keysState.find(e => e.contype === 'c' && e.relname === tableName && e.conkeys.includes(`{${fieldName}}`));
    const locateUKey = (tableName: string, fieldName: string) => keysState.find(e => e.contype === 'u' && e.relname === tableName && e.conkeys.includes(`{${fieldName}}`));
    const locatePKey = (tableName: string, fieldName: string) => keysState.find(e => e.contype === 'p' && e.relname === tableName && e.conkeys.includes(`{${fieldName}}`));
    const locateFKey = (tableName: (string | undefined), fieldName: (string | undefined), rTableName: (string | undefined), rFieldName: (string | undefined)) => keysState.find(e => (e.contype === 'f')
                                                                                                                                                                                && (e.relname === tableName || tableName === undefined)
                                                                                                                                                                                && (e.relfname === rTableName || rTableName === undefined)
                                                                                                                                                                                && (e.conkeys.includes(`{${fieldName ?? ''}}`) || fieldName === undefined)
                                                                                                                                                                                && (e.confkeys.includes(`{${rFieldName ?? ''}}`) || rFieldName === undefined));

    const alterFieldTypeResolvable = (from: FieldType | undefined, to: FieldType): Resolvable => {
        const mapEntry = FIELD_TYPE_MAP.find(e => e.jstype === from);
        if (libschema.isAssigned(mapEntry)) {
            const convertable = mapEntry.convertsTo.includes(to);
            if (convertable) {
                return { resolvable: true };
            } else {
                return {
                    resolvable: false,
                    description: `unable to convert field type from "${from}" to "${to}"`
                }
            }
        } else {
            return {
                resolvable: false,
                description: `unable to convert field type from "${from}" to "${to}"`
            }
        }
    }
    const alterFieldNullableResolvable = async (table: Table, field: Field): Promise<Resolvable> => {
        if (libschema.ifNotAssigned(field.nullable, false) === true) {
            return { resolvable: true };
        } else {
            if ((await query(`select 1 from "${schema}"."${table.name}" where "${field.name}" is null limit 1`)).rowCount === 0) {
                return { resolvable: true };
            } else {
                return {
                    resolvable: false,
                    description: `nulls found`
                };
            }
        }
    }
    const createFieldResolvable = async (table: Table, field: Field): Promise<Resolvable> => {
        if (libschema.ifNotAssigned(field.nullable, false) === true) {
            return { resolvable: true };
        } else {
            if (libschema.isAssigned(field.defaultValue)) {
                return { resolvable: true };
            } else {
                if ((await query(`select 1 from "${schema}"."${table.name}" limit 1`)).rowCount === 0) {
                    return { resolvable: true };
                } else {
                    return {
                        resolvable: false,
                        description: `rows found, but no default value set`
                    }
                }
            }
        }
    }
    const createFieldKeyResolvable = async (table: Table, field: Field): Promise<Resolvable> => {
        if (libschema.isAssigned(locateField(table.name, field.name))) {
            return { resolvable: true };
        } else if (libschema.isAssigned(locateField(table.name))) {
            if (result.issues.some(i => i.type === SyncIssueType.fieldNotFound
                                    && (i.table?.name === table.name)
                                    && (i.field?.name === field.name))) {
                return { resolvable: true };
            } else {
                return {
                    resolvable: false,
                    description: `field not found, nor issue with type "fieldNotFound"`
                }
            }
        } else {
            if (result.issues.some(i => i.type === SyncIssueType.tableNotFound
                                    && (i.table?.name === table.name))) {
                return { resolvable: true };
            } else {
                return {
                    resolvable: false,
                    description: `table not found, nor issue with type "tableNotFound"`
                }
            }
        }
    }
    const createUniqueKeyResolvable = async (table: Table, field: Field): Promise<Resolvable> => {
        if (libschema.isAssigned(locateField(table.name, field.name))) {
            const res = (await query(`select count(distinct "${field.name}") as "unique", count("${field.name}") as "all" from "${schema}"."${table.name}"`)).rows[0];
            if (res.unique === res.all) {
                return { resolvable: true };
            } else {
                return {
                    resolvable: false,
                    description: `non-unique values found`
                }
            }
        } else {
            return await createFieldKeyResolvable(table, field);
        }
    }
    const createCheckKeyResolvable = async (table: Table, field: Field): Promise<Resolvable> => {
        if (libschema.isAssigned(locateField(table.name, field.name))) {
            if ((await query(`select 1 from "${schema}"."${table.name}" where "${field.name}" not in (${field.oneOf?.map(v => valueExpr(v, field.type)).join(', ')}) limit 1`)).rowCount === 0) {
                return { resolvable: true };
            } else {
                return {
                    resolvable: false,
                    description: `out of list values found`
                }
            }
        } else {
            return await createFieldKeyResolvable(table, field);
        }
    }

    const createSchema = (): string[] => [`create schema "${schema}"`];
    const createTable = (table: Table): string[] => [`create table "${schema}"."${table.name}" (${table.fields.map(field => `"${field.name}" ${toDBType(field.type)} ${libschema.ifNotAssigned(field.nullable, false) === false ? 'not null' : ''}${field.defaultValue !== undefined ? ` default ${valueExpr(field.defaultValue, field.type)}` : ''}`).join(', ')})`];
    const createField = (table: Table, field: Field): string[] => [`alter table "${schema}"."${table.name}" add column "${field.name}" ${toDBType(field.type)} ${libschema.ifNotAssigned(field.nullable, false) === false ? 'not null' : ''}${field.defaultValue !== undefined ? ` default ${valueExpr(field.defaultValue, field.type)}` : ''}`];
    const alterFieldType = (table: Table, field: Field): string[] => [`alter table "${schema}"."${table.name}" alter column "${field.name}" type ${toDBType(field.type)}`];
    const alterFieldNullable = (table: Table, field: Field): string[] => [`alter table "${schema}"."${table.name}" alter column "${field.name}" ${libschema.ifNotAssigned(field.nullable, false) === false ? 'set' : 'drop'} not null`];
    const alterFieldDefaultValue = (table: Table, field: Field): string[] => {
        const action = libschema.isAssigned(field.defaultValue) ? 'set' : 'drop';
        const literal = libschema.isAssigned(field.defaultValue) ? ((field.type === 'string') ? `'${field.defaultValue.toString()}'` : `${field.defaultValue.toString()}`) : '';
        return [`alter table "${schema}"."${table.name}" alter column "${field.name}" ${action} default ${literal}`];
    }
    const createPrimaryKey = (table: Table, field: Field): string[] => [`alter table "${schema}"."${table.name}" add primary key ("${field.name}")`];
    const createForeignKey = (table: Table, field: Field): string[] => {
        const __ref = libschema.assert<FieldReference>(field.references, { type: 'object', arbitrary: true });
        const onDelete = libschema.isAssigned(__ref.onDelete) ? ` on delete ${__ref.onDelete}` : '';
        const onUpdate = libschema.isAssigned(__ref.onUpdate) ? ` on update ${__ref.onUpdate}` : '';
        return [`alter table "${schema}"."${table.name}" add constraint "${table.name}_${field.name}_${__ref.table}_${__ref.field}_fkey" foreign key ("${field.name}") references "${schema}"."${__ref.table}"("${__ref.field}")${onDelete}${onUpdate}`];
    }
    const createUniqueKey = (table: Table, field: Field): string[] => [`alter table "${schema}"."${table.name}" add constraint "${table.name}_${field.name}_unique" unique ("${field.name}")`];
    const createCheckKey = (table: Table, field: Field): string[] => [`alter table "${schema}"."${table.name}" add constraint "${table.name}_${field.name}_check" check ("${field.name}" in (${(field.oneOf ?? []).map(v => valueExpr(v, field.type)).join(', ')}))`];
    const dropConstraint = (table: Table, name: string): string[] => [`alter table "${schema}"."${table.name}" drop constraint "${name}"`];
    const createIndex = (table: Table, field: Field, type: FieldIndexType): string[] => [`create index "${table.name}_${field.name}_${type}" on "${schema}"."${table.name}" using ${type} (${field.name})`];
    const dropIndex = (name: string): string[] => [`drop index "${schema}"."${name}"`];

    for (const table of tables) {
        const pkFields = table.fields.filter(f => f.primaryKey === true);
        if (pkFields.length > 1) {
            throw new Error(`more than one primary key fields found on table "${table.name}"`);
        }
        if (pkFields.length === 1) {
            if (libschema.ifNotAssigned(pkFields[0].nullable, false) === true) {
                throw new Error(`primary key field "${table.name}.${pkFields[0].name}" cannot be nullable`);
            }
        }
        for (const field of table.fields) {
            if (field.type === 'serial') {
                if (libschema.isAssigned(field.nullable)) {
                    throw new Error(`leave "nullable" unset when field type is set to "serial"`);
                }
                if (libschema.isAssigned(field.unique)) {
                    throw new Error(`leave "unique" unset when field type is set to "serial"`);
                }
            }
            if (libschema.isAssigned(field.references)) {
                const __ref = field.references;
                const refTable = tables.find(t => t.name === __ref.table);
                if (libschema.isAssigned(refTable)) {
                    const refField = refTable.fields.find(f => f.name === __ref.field);
                    if (libschema.isAssigned(refField)) {
                        if (refField.primaryKey !== true) {
                            throw new Error(`invalid reference "${table.name}.${field.name}", referenced field "${__ref.table}.${__ref.field}" has no primary key on it`);
                        }
                        const lclass = libschema.assert<string>(FIELD_TYPE_MAP.find(e => e.jstype === field.type)?.class_, { type: 'string' });
                        const rclass = libschema.assert<string>(FIELD_TYPE_MAP.find(e => e.jstype === refField.type)?.class_, { type: 'string' });
                        if (lclass !== rclass) {
                            throw new Error(`invalid reference "${table.name}.${field.name}", referenced field "${__ref.table}.${__ref.field}" has different data type class "${rclass}"`);
                        }
                    } else {
                        throw new Error(`invalid reference "${table.name}.${field.name}", referenced table "${__ref.table}" has no field "${__ref.field}"`);
                    }
                } else {
                    throw new Error(`reference table "${__ref.table}" not found for field "${field.name}" of table "${table.name}"`);
                }
            }
            if (libschema.isAssigned(field.oneOf) && field.oneOf.length === 0) {
                throw new Error(`"oneOf" property must contain more than one element in field "${table.name}.${field.name}"`);
            }
            if (libschema.isAssigned(field.index)) {
                if (field.type === 'serial') {
                    throw new Error(`unable to set "index" property for field with type "serial" for field "${table.name}.${field.name}"`);
                }
                if (libschema.isAssigned(field.references)) {
                    throw new Error(`unable to set "index" property for field "${table.name}.${field.name}" that references another table`);
                }
                if (libschema.ifNotAssigned(field.unique, false) === true) {
                    throw new Error(`unable to set "index" property for unique field "${table.name}.${field.name}"`);
                }
                if (libschema.ifNotAssigned(field.primaryKey, false) === true) {
                    throw new Error(`unable to set "index" property for primary key field "${table.name}.${field.name}"`);
                }
            }
        }
    }

    if ((await query(SQL_SCHEMA_LIST)).rows.map(r => r.nspname).includes(schema)) {
        for (const table of tables) {
            const tableState = locateField(table.name);
            if (libschema.isAssigned(tableState)) {
                for (const field of table.fields) {
                    const fieldState = locateField(table.name, field.name);
                    if (libschema.isAssigned(fieldState)) {
                        const fieldType = toJSType(fieldState.typname);
                        const fieldNullable = (fieldState.attnotnull === false);
                        const fieldDefaultValue = await (async () => {
                            const expr = libschema.ifNotAssigned(fieldState.adbin_sql, '').toString();
                            if (expr.length > 0) {
                                return (await query(`select ${expr} as "value"`)).rows[0].value.toString();
                            } else {
                                return '';
                            }
                        })();
                        if ((field.type !== fieldType)
                        && ((field.type !== 'serial') || (fieldType !== 'integer'))) {
                            result.issues.push({
                                type: SyncIssueType.fieldTypeMissmatch,
                                schema: schema,
                                table: table,
                                field: field,
                                actions: alterFieldType(table, field),
                                ...(alterFieldTypeResolvable(fieldType, field.type))
                            });
                        }
                        if (field.type !== 'serial') {
                            if (libschema.ifNotAssigned(field.nullable, false) !== fieldNullable) {
                                result.issues.push({
                                    type: SyncIssueType.fieldNullableMissmatch,
                                    schema: schema,
                                    table: table,
                                    field: field,
                                    actions: alterFieldNullable(table, field),
                                    ...(await alterFieldNullableResolvable(table, field))
                                });
                            }
                            if (libschema.ifNotAssigned(field.defaultValue, '').toString() !== fieldDefaultValue) {
                                result.issues.push({
                                    type: SyncIssueType.fieldDefaultValueMissmatch,
                                    schema: schema,
                                    table: table,
                                    field: field,
                                    actions: alterFieldDefaultValue(table, field),
                                    resolvable: true
                                });
                            }
                        }
                    } else {
                        result.issues.push({
                            type: SyncIssueType.fieldNotFound,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: createField(table, field),
                            ...(await createFieldResolvable(table, field))
                        });
                    }
                }
            } else {
                result.issues.push({
                    type: SyncIssueType.tableNotFound,
                    schema: schema,
                    table: table,
                    actions: createTable(table),
                    resolvable: true
                });
            }
        }

        for (const table of tables) {
            for (const field of table.fields) {
                const pkeyOn = libschema.ifNotAssigned(field.primaryKey, false);
                const pkeyState = locatePKey(table.name, field.name);
                const ukeyState = locateUKey(table.name, field.name);
                const ckeyState = locateCKey(table.name, field.name);
                const btreeState = locateBTree(table.name, field.name);

                if (pkeyOn) {
                    if (libschema.isNotAssigned(pkeyState)) {
                        result.issues.push({
                            type: SyncIssueType.primaryKeyNotFound,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: createPrimaryKey(table, field),
                            resolvable: true
                        });
                    }
                } else {
                    if (libschema.isAssigned(pkeyState)) {
                        const fkeyState = locateFKey(undefined, undefined, table.name, field.name);
                        result.issues.push({
                            type: SyncIssueType.primaryKeyDangling,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: dropConstraint(table, pkeyState.conname),
                            resolvable: libschema.isNotAssigned(fkeyState),
                            description: libschema.isNotAssigned(fkeyState) ? undefined : `field "${table.name}"."${field.name}" is referencing to this primary key`
                        });
                    }
                }

                if (libschema.isAssigned(field.references)) {
                    const __ref = libschema.assert<FieldReference>(field.references, { type: 'object', arbitrary: true });
                    const fkeyState = locateFKey(table.name, field.name, __ref.table, __ref.field);
                    if (libschema.isAssigned(fkeyState)) {
                        const refType2Char = (type: FieldReferenceOnAction | undefined) => {
                            if (type === 'cascade') {
                                return 'c';
                            } else if (type === 'no action') {
                                return 'a';
                            } else if (type === 'restrict') {
                                return 'r';
                            } else if (type === 'set default') {
                                return 'd'
                            } else {
                                return 'a'
                            }
                        }

                        const ondel = refType2Char(__ref.onDelete);
                        const onupd = refType2Char(__ref.onUpdate);

                        if ((fkeyState.confdeltype !== ondel)
                        || (fkeyState.confupdtype !== onupd)) {
                            result.issues.push({
                                type: SyncIssueType.foreignKeyMissmatch,
                                schema: schema,
                                table: table,
                                field: field,
                                actions: [
                                    ...dropConstraint(table, fkeyState.conname),
                                    ...createForeignKey(table, field)
                                ],
                                resolvable: true
                            });
                        }
                    } else {
                        const refPKeyState = locatePKey(__ref.table, __ref.field);
                        const refFieldState = locateField(__ref.table, __ref.field);
                        const refTableIssue = result.issues.find(i => i.type === SyncIssueType.tableNotFound
                                                                && i.resolvable === true
                                                                && i.schema === schema
                                                                && i.table?.name === __ref.table
                                                                && i.table?.fields.some(f => f.name === __ref.field && f.primaryKey === true));
                        const refFieldIssue = result.issues.find(i => i.type === SyncIssueType.fieldNotFound
                                                                && i.resolvable === true
                                                                && i.schema === schema
                                                                && i.table?.name === __ref.table
                                                                && i.field?.name === __ref.field
                                                                && i.field?.primaryKey === true);
                        const refPKeyIssue = result.issues.find(i => i.type === SyncIssueType.primaryKeyNotFound
                                                                && i.resolvable === true
                                                                && i.schema === schema
                                                                && i.table?.name === __ref.table
                                                                && i.field?.name === __ref.field
                                                                && i.field?.primaryKey === true);
                        result.issues.push({
                            type: SyncIssueType.foreignKeyNotFound,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: createForeignKey(table, field),
                            resolvable: (libschema.isAssigned(refPKeyState) && libschema.isAssigned(refFieldState))
                                    || (libschema.isAssigned(refTableIssue))
                                    || (libschema.isAssigned(refFieldIssue) || libschema.isAssigned(refPKeyIssue))
                        });
                    }
                } else {
                    const fkeyState = locateFKey(table.name, field.name, undefined, undefined);
                    if (libschema.isAssigned(fkeyState)) {
                        result.issues.push({
                            type: SyncIssueType.foreignKeyDangling,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: dropConstraint(table, fkeyState.conname),
                            resolvable: true
                        });
                    }
                }

                if (field.unique === true) {
                    if (libschema.isNotAssigned(ukeyState)) {
                        result.issues.push({
                            type: SyncIssueType.uniqueKeyNotFound,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: createUniqueKey(table, field),
                            ...(await createUniqueKeyResolvable(table, field))
                        });
                    }
                } else {
                    if (libschema.isAssigned(ukeyState)) {
                        result.issues.push({
                            type: SyncIssueType.uniqueKeyDangling,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: dropConstraint(table, ukeyState.conname),
                            resolvable: true
                        });
                    }
                }

                if (libschema.isAssigned(field.oneOf)) {
                    if (libschema.isAssigned(ckeyState)) {
                        if (!field.oneOf.every(v => ckeyState.condef.includes(valueExpr(v, field.type)))) {
                            result.issues.push({
                                type: SyncIssueType.checkKeyMissmatch,
                                schema: schema,
                                table: table,
                                field: field,
                                actions: [...dropConstraint(table, ckeyState.conname), ...createCheckKey(table, field)],
                                ...(await createCheckKeyResolvable(table, field))
                            });
                        }
                    } else {
                        result.issues.push({
                            type: SyncIssueType.checkKeyNotFound,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: createCheckKey(table, field),
                            resolvable: true
                        });
                    }
                } else {
                    if (libschema.isAssigned(ckeyState)) {
                        result.issues.push({
                            type: SyncIssueType.checkKeyDangling,
                            schema: schema,
                            table: table,
                            field: field,
                            actions: dropConstraint(table, ckeyState.conname),
                            resolvable: true
                        });
                    }
                }

                if ((libschema.ifNotAssigned(field.primaryKey, false) !== true)
                && (libschema.ifNotAssigned(field.unique, false) !== true)
                && (libschema.isNotAssigned(field.references))
                && (field.type !== 'serial')) {
                    if (field.index === 'btree') {
                        if (libschema.isNotAssigned(btreeState)) {
                            result.issues.push({
                                type: SyncIssueType.btreeIndexNotFound,
                                schema: schema,
                                table: table,
                                field: field,
                                actions: createIndex(table, field, 'btree'),
                                ...(await createFieldKeyResolvable(table, field))
                            });
                        }
                    } else {
                        if (libschema.isAssigned(btreeState)) {
                            result.issues.push({
                                type: SyncIssueType.btreeIndexDangling,
                                schema: schema,
                                table: table,
                                field: field,
                                actions: dropIndex(btreeState.idxname),
                                resolvable: true
                            });
                        }
                    }
                }
            }
        }
    } else {
        result.issues.push({
            type: SyncIssueType.schemaNotFound,
            schema: schema,
            actions: createSchema(),
            resolvable: true
        });

        for (const table of tables) {
            result.issues.push({
                type: SyncIssueType.tableNotFound,
                schema: schema,
                table: table,
                actions: createTable(table),
                resolvable: true
            });
            for (const field of table.fields) {
                if (field.primaryKey === true) {
                    result.issues.push({
                        type: SyncIssueType.primaryKeyNotFound,
                        schema: schema,
                        table: table,
                        field: field,
                        actions: createPrimaryKey(table, field),
                        resolvable: true
                    });
                }
                if (libschema.isAssigned(field.references)) {
                    result.issues.push({
                        type: SyncIssueType.foreignKeyNotFound,
                        schema: schema,
                        table: table,
                        field: field,
                        actions: createForeignKey(table, field),
                        resolvable: true
                    });
                }
                if (field.unique === true) {
                    result.issues.push({
                        type: SyncIssueType.uniqueKeyNotFound,
                        schema: schema,
                        table: table,
                        field: field,
                        actions: createUniqueKey(table, field),
                        resolvable: true
                    });
                }
                if (field.oneOf !== undefined) {
                    result.issues.push({
                        type: SyncIssueType.checkKeyNotFound,
                        schema: schema,
                        table: table,
                        field: field,
                        actions: createCheckKey(table, field),
                        resolvable: true
                    });
                }
                if (field.index === 'btree') {
                    result.issues.push({
                        type: SyncIssueType.btreeIndexNotFound,
                        schema: schema,
                        table: table,
                        field: field,
                        actions: createIndex(table, field, 'btree'),
                        resolvable: true
                    });
                }
            }
        }
    }

    result.actions = result.issues.reduce<string[]>((ret, i) => [...ret, ...i.actions], []);
    result.resolvable = (result.issues.length === 0) || result.issues.every(i => i.resolvable === true);
    result.fulfilled = (result.issues.length === 0);

    if (result.resolvable
    && !result.fulfilled
    && (options?.fullfill === true)) {
        for (const issueType of [SyncIssueType.schemaNotFound, SyncIssueType.tableNotFound,
                                SyncIssueType.fieldNotFound, SyncIssueType.fieldTypeMissmatch,
                                SyncIssueType.fieldNullableMissmatch, SyncIssueType.fieldDefaultValueMissmatch,
                                SyncIssueType.primaryKeyNotFound, SyncIssueType.primaryKeyDangling,
                                SyncIssueType.foreignKeyNotFound, SyncIssueType.foreignKeyDangling,
                                SyncIssueType.foreignKeyMissmatch, SyncIssueType.uniqueKeyNotFound,
                                SyncIssueType.uniqueKeyDangling, SyncIssueType.checkKeyNotFound,
                                SyncIssueType.checkKeyMissmatch, SyncIssueType.checkKeyDangling,
                                SyncIssueType.btreeIndexNotFound, SyncIssueType.btreeIndexDangling]) {
            for (const issue of result.issues) {
                if (issue.type === issueType) {
                    for (const action of issue.actions) {
                        try {
                            await query(action);
                            console.log(`action applied ${action}`);
                            issue.fulfilled = true;
                        } catch (e) {
                            issue.error = e;
                            issue.fulfilled = false;
                        }
                    }
                }
            }
        }
        result.fulfilled = result.issues.every(i => i.fulfilled === true);
    }

    const fieldState2 = (await client.query<SchemaFieldsEntry>(SQL_SCHEMA_FIELDS, [schema])).rows;
    const tableState2: [ string, string[] ][] = [];

    for (const entry of fieldState2) {
        const tab = tableState2.find(t => t[0] === entry.relname);
        if (libschema.isAssigned(tab)) {
            tab[1].push(entry.attname);
        } else {
            tableState2.push([ entry.relname, [entry.attname] ]);
        }
    }

    for (const tab of tableState2) {
        const table = tables.find(t => t.name === tab[0]);
        if (libschema.isAssigned(table)) {
            for (const fld of tab[1]) {
                const field = table.fields.find(f => f.name === fld);
                if (libschema.isNotAssigned(field)) {
                    result.warnings.push(`Field "${fld}" of table "${tab[0]}" not presented in schema "${schema}"`);
                }
            }
        } else {
            result.warnings.push(`Table "${tab[0]}" not presented in schema "${schema}"`);
        }
    }

    return result;
}