module ActiveRecord
  module ConnectionAdapters
    module OracleEnhanced
      module BuildSchemaCache

        def build_indexes_cache()
          sql = <<~SQL.squish
          SELECT 
          LOWER(i.table_name) AS table_name, 
          LOWER(i.index_name) AS index_name, 
          i.uniqueness,
          i.index_type, 
          i.ityp_owner, 
          i.ityp_name, 
          i.parameters,
          LOWER(i.tablespace_name) AS tablespace_name,
          LOWER(c.column_name) AS column_name, e.column_expression,
          atc.virtual_column
          FROM all_indexes i
          JOIN all_ind_columns c ON c.index_name = i.index_name AND c.index_owner = i.owner
          LEFT OUTER JOIN all_ind_expressions e ON e.index_name = i.index_name AND
                          e.index_owner = i.owner AND e.column_position = c.column_position
          LEFT OUTER JOIN all_tab_cols atc ON i.table_name = atc.table_name AND
                          c.column_name = atc.column_name AND i.owner = atc.owner AND atc.hidden_column = 'NO'
          WHERE i.owner = SYS_CONTEXT('userenv', 'current_schema')
          AND i.table_owner = SYS_CONTEXT('userenv', 'current_schema')
          AND NOT EXISTS (SELECT uc.index_name FROM all_constraints uc
                          WHERE uc.index_name = i.index_name AND uc.owner = i.owner AND uc.constraint_type = 'P')
          ORDER BY i.table_name,i.index_name, c.column_position
          SQL
          current_table = nil
          cache = {}
          select_all(sql).each do |row|
            # have to keep track of indexes because above query returns dups
            # there is probably a better query we could figure out
            if current_table != row["table_name"]
              current_table = row["table_name"]
              all_table_indexes = []
              current_index = nil
            end
            if current_index != row["index_name"]
              current_index = row["index_name"]
              all_table_indexes << build_index_definition(row)
            end
            # Functional index columns and virtual columns both get stored as column expressions,
            # but re-creating a virtual column index as an expression (instead of using the virtual column's name)
            # results in a ORA-54018 error.  Thus, we only want the column expression value returned
            # when the column is not virtual.
            if row["column_expression"] && row["virtual_column"] != "YES"
              all_table_indexes.last.columns << row["column_expression"]
            else
              all_table_indexes.last.columns << row["column_name"].downcase
            end
            cache[current_table] = all_table_indexes
          end
          cache
        end

        def build_index_definition(row)
          statement_parameters = nil
          if row["index_type"] == "DOMAIN" && row["ityp_owner"] == "CTXSYS" && row["ityp_name"] == "CONTEXT"
            procedure_name = default_datastore_procedure(row["index_name"])
            source = select_values(<<~SQL.squish, "SCHEMA", [bind_string("procedure_name", procedure_name.upcase)]).join
              SELECT text
              FROM all_source
              WHERE owner = SYS_CONTEXT('userenv', 'current_schema')
                AND name = :procedure_name
              ORDER BY line
            SQL
            if source =~ /-- add_context_index_parameters (.+)\n/
              statement_parameters = $1
            end
          end
          OracleEnhanced::IndexDefinition.new(
            row["table_name"],
            row["index_name"],
            row["uniqueness"] == "UNIQUE",
            [],
            {},
            row["index_type"] == "DOMAIN" ? "#{row['ityp_owner']}.#{row['ityp_name']}" : nil,
            row["parameters"],
            statement_parameters,
            row["tablespace_name"] == default_tablespace ? nil : row["tablespace_name"])
        end

      end
    end
  end
end
