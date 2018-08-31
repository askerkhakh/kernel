package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import java.util.ArrayList;
import java.util.List;

public class Normalizer {
	protected static final List<SqlObjectNormalizer> NORMALIZERS = getNormalizers();

	public Normalizer() {
	}
	/* Нормализовать дерево sql-объектов.
		  На данный момент нормализация заключается только лишь в
		  настройке порядка элементов в дереве.
		  (и в оптимизации "скобок" условий )
	*/
	
	public static void normalize(SqlObject root)
            throws SqlObjectException {
		if (root == null) {
			return;
		}
		for (SqlObject item: root) {
			normalize(item);
		}
		SqlObjectNormalizer normalizer = getNormalizer(root);
		if (normalizer != null) {
			normalizer.normalize(root);
		}
	}

	protected abstract static class SqlObjectNormalizer {

		public abstract void normalize(SqlObject comp)
				throws SqlObjectException;
		public abstract boolean isMy(SqlObject comp);
	}

    protected static class ConditionsNormalizer extends SqlObjectNormalizer {

        private List<Conditions> bracketsToDelete = new ArrayList<>();

        @Override
        public boolean isMy(SqlObject comp) {
            return comp instanceof Conditions;
        }

        protected void collectEmptyBrackets(Conditions bracket) {
            for (SqlObject item: bracket)
                if (item.getClass() == Conditions.class) {
                    Conditions innerBracket = (Conditions) item;
                    if (innerBracket.isEmpty())
                        bracketsToDelete.add(innerBracket);
                    else
                        collectEmptyBrackets(innerBracket);
                }
        }

        protected void deleteEmptyBrackets(Conditions bracket)
                throws SqlObjectException {
            collectEmptyBrackets(bracket);
            for(Conditions item: bracketsToDelete)
                item.getOwner().removeItem(item);
        }

        protected void optimizeBrackets (Conditions bracket)
                throws SqlObjectException {
            List<Predicate> extractedItems = new ArrayList<>();
            for (SqlObject item: bracket)
                if (item.getClass() == Conditions.class) {
                    Conditions innerBracket = (Conditions) item;
                    optimizeBrackets(innerBracket);
                    if (innerBracket.itemsCount() == 1) {
                        // вложенная скобка содержит только одно условие - возьмём его...
                        Predicate predicate = (Predicate) innerBracket.firstSubItem();
                        // ...применив к нему отрицание скобки
                        predicate.not = innerBracket.not ^ predicate.not;
                        extractedItems.add(predicate);
                    }
                    else if (!innerBracket.not && bracket.booleanOp == innerBracket.booleanOp)
                        // вложенная скобка не имеет отрицания и её логическая операция совпадает с охватывающей
                        for(SqlObject innerItem: innerBracket)
                            // поднимем все условия скобки на уровень выше
                            extractedItems.add((Predicate) innerItem);
                }

            for (Predicate item: extractedItems) {
                SqlObject ownerBracket = item.getOwner();
                if (ownerBracket.getOwner() != null) {
                    Preconditions.checkState(ownerBracket.getOwner() == bracket);
                    bracket.removeItem(ownerBracket);
                }
                bracket.insertItem(item);
            }
        }

        @Override
        public void normalize(SqlObject comp)
                throws SqlObjectException {
            Conditions topBracket = (Conditions)comp;
            bracketsToDelete.clear();

            deleteEmptyBrackets(topBracket);
            optimizeBrackets(topBracket);

            /* если в данной группе условий остался по-прежнему 1 элемент,
                принудительно выставим данной группе условий логическую операцию AND.
                Может помочь при избавлении от трансферов */
            if (topBracket.itemsCount() == 1)
                topBracket.booleanOp = Conditions.BooleanOp.AND;
        }
    }

	protected static List<SqlObjectNormalizer> getNormalizers() {
		List<SqlObjectNormalizer> result = new ArrayList<SqlObjectNormalizer>();
		result.add(new ConditionsNormalizer());
		return result;
	}

	protected static SqlObjectNormalizer getNormalizer(SqlObject item) {
		for (SqlObjectNormalizer result: NORMALIZERS) {
			if (result.isMy(item)) {
				return result;
			}
		}
		return null;
	}


}
