import { Field, Table } from './model'
import {
    Expr,
    DoubleExpr,
    IntegerExpr,
    StringExpr,
    useIntegerExpr,
    useDoubleExpr,
    useStringExpr,
    useIntegerNullableExpr,
    useDoubleNullableExpr,
    useStringNullableExpr,
    NullableIntegerExpr,
    NullableDoubleExpr,
    NullableStringExpr,
    BooleanExpr,
    NullableBooleanExpr
} from './expr'

type ScopeTableField<T extends Field> = {
    [K in T['name']]: (
        T['nullable'] extends true ? (
            T['type'] extends 'serial' ? NullableIntegerExpr :
            T['type'] extends 'integer' ? NullableIntegerExpr :
            T['type'] extends 'double' ? NullableDoubleExpr :
            T['type'] extends 'string' ? NullableStringExpr :
            never
        ) : (
            T['type'] extends 'serial' ? IntegerExpr :
            T['type'] extends 'integer' ? IntegerExpr :
            T['type'] extends 'double' ? DoubleExpr :
            T['type'] extends 'string' ? StringExpr :
            never
        )
    )
}

type ScopeTable<T extends readonly Field[]> = (
    T extends readonly [ infer Head extends Field ] ? ScopeTableField<Head> :
    T extends readonly [ infer Head extends Field, ...infer Tail extends Field[] ] ? ScopeTableField<Head> & ScopeTable<Tail> :
    never
)

type Scope<T extends Table, A extends string> = {
    [K in A]: ScopeTable<T['fields']>;
}

type Result<T extends { [K: string]: Expr; }> = {
    [K in keyof T]: (
        T[K] extends BooleanExpr ? boolean :
        T[K] extends IntegerExpr ? number :
        T[K] extends DoubleExpr ? number :
        T[K] extends StringExpr ? string :
        T[K] extends NullableBooleanExpr ? boolean | null :
        T[K] extends NullableIntegerExpr ? number | null :
        T[K] extends NullableDoubleExpr ? number | null :
        T[K] extends NullableStringExpr ? string | null :
        never
    )
}[]

type Context = {
    select: {
        [K: string]: Expr;
    },
    from: {
        table: Table;
        alias: string;
    },
    join: {
        table: Table;
        alias: string;
        expr: Expr;
    }[],
    scope: {
        [K: string]: {
            [K: string]: Expr;
        }
    },
    where?: Expr;
    orderBy?: Expr | Expr[];
}

type QueryExecOptions = {
    pageSize?: number;
    pageIndex?: number;
}

const fieldToExpr = (alias: string, field: Field) => {
    const expr = `${alias}."${field.name}"`;
    if (field.nullable === true) {
        if (field.type === 'serial') return useIntegerNullableExpr(expr);
        else if (field.type === 'integer') return useIntegerNullableExpr(expr);
        else if (field.type === 'double') return useDoubleNullableExpr(expr);
        else if (field.type === 'string') return useStringNullableExpr(expr);
        else throw new Error('fieldToExpr() failed: Wrong field type');
    } else {
        if (field.type === 'serial') return useIntegerExpr(expr);
        else if (field.type === 'integer') return useIntegerExpr(expr);
        else if (field.type === 'double') return useDoubleExpr(expr);
        else if (field.type === 'string') return useStringExpr(expr);
        else throw new Error('fieldToExpr() failed: Wrong field type');
    }
}

const useExec = <R>(context: Context) => ({
    exec: async (options?: QueryExecOptions) => {
        console.log(context);

        return {} as R;
    }
})

const useOrderBy = <S, R>(context: Context) => ({
    orderBy: (expr: (scope: S) => Expr | Expr[]) => {
        context.orderBy = expr(context.scope as S)
        return {
            ...useExec<R>(context)
        }
    }
})

const useWhere = <S, R>(context: Context) => ({
    where: (expr: (scope: S) => Expr) => {
        context.where = expr(context.scope as S);
        return {
            ...useOrderBy<S, R>(context),
            ...useExec<R>(context)
        }
    }
})

const useSelect = <S>(context: Context) => ({
    select: <R extends { [K: string]: Expr; }>(expr: (scope: S) => R) => {
        context.select = expr(context.scope as S);
        return {
            ...useWhere<S, Result<R>>(context),
            ...useOrderBy<S, Result<R>>(context),
            ...useExec<Result<R>>(context)
        }
    }
})

const useJoin = <S>(context: Context) => ({
    join: <T extends Table, A extends string>(table: T, alias: A, expr: (scope: S & Scope<T, A>) => Expr) => {
        context.scope = {
            ...context.scope,
            [alias]: (
                table.fields.reduce((ret, field) => ({
                    ...ret,
                    [field.name]: fieldToExpr(alias, field)
                }), {})
            )
        }
        context.join = [
            ...context.join,
            { table, alias, expr: expr(context.scope as S & Scope<T, A>) }
        ]
        return {
            ...useJoin<S & Scope<T, A>>(context),
            ...useSelect<S & Scope<T, A>>(context)
        }
    }
})

const useFrom = () => ({
    from: <T extends Table, A extends string>(table: T, alias: A) => {
        const context: Context = {
            select: {},
            from: { table, alias },
            join: [],
            scope: {
                [alias]: (
                    table.fields.reduce((ret, field) => ({
                        ...ret,
                        [field.name]: fieldToExpr(alias, field)
                    }), {})
                )
            }
        }
        return {
            ...useJoin<Scope<T, A>>(context),
            ...useSelect<Scope<T, A>>(context)
        }
    }
})

export type QueryOptions = {

}

export const query = (options?: QueryOptions) => {

    return {
        ...useFrom()
    }
}