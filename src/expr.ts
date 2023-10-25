export type Expr = {
    readonly expr: () => string;
}

export type GenericExpr = Expr & {
    readonly asc: () => Expr;
    readonly desc: () => Expr;
    readonly cast: (
        ((type: 'integer') => IntegerExpr) &
        ((type: 'double') => DoubleExpr) &
        ((type: 'string') => StringExpr)
    )
}

export type NullableBooleanExpr = GenericExpr & {
    readonly ifNull: (expr: boolean | BooleanExpr) => BooleanExpr;
}

export type NullableDoubleExpr = GenericExpr & {
    readonly ifNull: (expr: number | DoubleExpr) => DoubleExpr;
}

export type NullableIntegerExpr = GenericExpr & {
    readonly ifNull: (expr: number | IntegerExpr) => IntegerExpr;
}

export type NullableStringExpr = GenericExpr & {
    readonly ifNull: (expr: string | StringExpr) => StringExpr;
}

export type BooleanExpr = GenericExpr & {
    readonly ifNull: (expr: boolean | BooleanExpr) => BooleanExpr;
    readonly not: () => BooleanExpr;
}

export type DoubleExpr = GenericExpr & {
    readonly ifNull: (expr: number | DoubleExpr) => DoubleExpr;
    readonly equals: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
    readonly lessThan: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
    readonly greaterThan: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
    readonly lessThanOrEqual: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
    readonly greaterThanOrEqual: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
}

export type IntegerExpr = GenericExpr & {
    readonly ifNull: (expr: number | IntegerExpr) => IntegerExpr;
    readonly equals: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
    readonly lessThan: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
    readonly greaterThan: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
    readonly lessThanOrEqual: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
    readonly greaterThanOrEqual: (expr: DoubleExpr | IntegerExpr | number) => BooleanExpr;
}

export type StringExpr = GenericExpr & {
    readonly ifNull: (expr: string | StringExpr) => StringExpr;
    readonly equals: (expr: StringExpr | string) => BooleanExpr;
    readonly startsWith: (expr: StringExpr | string) => BooleanExpr;
    readonly endsWith: (expr: StringExpr | string) => BooleanExpr;
    readonly upper: () => StringExpr;
    readonly lower: () => StringExpr;
    readonly trim: () => StringExpr;
    readonly trimLeft: () => StringExpr;
    readonly trimRight: () => StringExpr;
}

export const exprOf = (value: Expr | string | number | boolean) => {
    if (typeof value === 'number') {
        return value.toString();
    } else if (typeof value === 'string') {
        return `'${value}'`;
    } else if (typeof value === 'boolean') {
        return (value ? 'true' : 'false');
    } else {
        return value.expr();
    }
}

export const useExpr = (baseExpr: string): Expr => ({
    expr: () => baseExpr
})

export const useGenericExpr = (baseExpr: string): GenericExpr => ({
    ...useExpr(baseExpr),
    asc: () => useExpr(`${baseExpr} asc`),
    desc: () => useExpr(`${baseExpr} desc`),
    cast: (type: any): any => {
        if (type === 'integer') return useIntegerExpr(`cast(${baseExpr} as bigint)`);
        else if (type === 'double') return useDoubleExpr(`cast(${baseExpr} as double)`);
        else if (type === 'string') return useStringExpr(`cast(${baseExpr} as varchar)`);
        else throw new Error('wrong cast type');
    }
})

export const useBooleanNullableExpr = (baseExpr: string): NullableBooleanExpr => ({
    ...useGenericExpr(baseExpr),
    ifNull: expr => useBooleanExpr(`coalesce(${baseExpr}, ${exprOf(expr)})`)
})

export const useIntegerNullableExpr = (baseExpr: string): NullableIntegerExpr => ({
    ...useGenericExpr(baseExpr),
    ifNull: expr => useIntegerExpr(`coalesce(${baseExpr}, ${exprOf(expr)})`)
})

export const useDoubleNullableExpr = (baseExpr: string): NullableDoubleExpr => ({
    ...useGenericExpr(baseExpr),
    ifNull: expr => useDoubleExpr(`coalesce(${baseExpr}, ${exprOf(expr)})`)
})

export const useStringNullableExpr = (baseExpr: string): NullableStringExpr => ({
    ...useGenericExpr(baseExpr),
    ifNull: expr => useStringExpr(`coalesce(${baseExpr}, ${exprOf(expr)})`)
})

export const useBooleanExpr = (baseExpr: string): BooleanExpr => ({
    ...useGenericExpr(baseExpr),
    ifNull: expr => useBooleanExpr(`coalesce(${baseExpr}, ${exprOf(expr)})`),
    not: () => useBooleanExpr(`(not ${baseExpr})`)
})

export const useIntegerExpr = (baseExpr: string): IntegerExpr => ({
    ...useGenericExpr(baseExpr),
    ifNull: expr => useIntegerExpr(`coalesce(${baseExpr}, ${exprOf(expr)})`),
    equals: expr => useBooleanExpr(`(${baseExpr} = ${exprOf(expr)})`),
    lessThan: expr => useBooleanExpr(`(${baseExpr} < ${exprOf(expr)})`),
    greaterThan: expr => useBooleanExpr(`(${baseExpr} > ${exprOf(expr)})`),
    lessThanOrEqual: expr => useBooleanExpr(`(${baseExpr} <= ${exprOf(expr)})`),
    greaterThanOrEqual: expr => useBooleanExpr(`(${baseExpr} >= ${exprOf(expr)})`)
})

export const useDoubleExpr = (baseExpr: string): DoubleExpr => ({
    ...useGenericExpr(baseExpr),
    ifNull: expr => useDoubleExpr(`coalesce(${baseExpr}, ${exprOf(expr)})`),
    equals: expr => useBooleanExpr(`(${baseExpr} = ${exprOf(expr)})`),
    lessThan: expr => useBooleanExpr(`(${baseExpr} < ${exprOf(expr)})`),
    greaterThan: expr => useBooleanExpr(`(${baseExpr} > ${exprOf(expr)})`),
    lessThanOrEqual: expr => useBooleanExpr(`(${baseExpr} <= ${exprOf(expr)})`),
    greaterThanOrEqual: expr => useBooleanExpr(`(${baseExpr} >= ${exprOf(expr)})`)
})

export const useStringExpr = (baseExpr: string): StringExpr => ({
    ...useGenericExpr(baseExpr),
    ifNull: expr => useStringExpr(`coalesce(${baseExpr}, ${exprOf(expr)})`),
    equals: expr => useBooleanExpr(`(${baseExpr} = ${exprOf(expr)})`),
    startsWith: expr => useBooleanExpr(`(${baseExpr} like (${exprOf(expr)} + '%'))`),
    endsWith: expr => useBooleanExpr(`(${baseExpr} like ('%' + ${exprOf(expr)}))`),
    upper: () => useStringExpr(`upper(${baseExpr})`),
    lower: () => useStringExpr(`lower(${baseExpr})`),
    trim: () => useStringExpr(`trim(${baseExpr})`),
    trimLeft: () => useStringExpr(`ltrim(${baseExpr})`),
    trimRight: () => useStringExpr(`rtrim(${baseExpr})`)
})

export type FunctionExpr = {
    readonly dateTrunc: (expr: IntegerExpr | number, to: 'month' | 'day' | 'hour' | 'minute' | 'second') => IntegerExpr;
    readonly dateFormat: (expr: IntegerExpr | number) => StringExpr;
}